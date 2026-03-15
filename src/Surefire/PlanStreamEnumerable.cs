using System.Text.Json;

namespace Surefire;

/// <summary>
/// An IAsyncEnumerable&lt;T&gt; that reads Output events from a streaming plan step's run.
/// Used when a downstream step depends on a streaming step via StreamRefValue —
/// the engine stores a $planStream marker in the arguments, and ParameterBinder
/// creates this enumerator to consume the source step's output directly.
/// </summary>
internal sealed class PlanStreamEnumerable<T>(
    string sourceRunId,
    IJobStore store,
    INotificationProvider notifications,
    CancellationToken cancellationToken,
    JsonSerializerOptions serializerOptions) : IAsyncEnumerable<T>
{
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken token = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, token);
        var ct = linkedCts.Token;

        using var wakeup = new SemaphoreSlim(0, 1);

        await using var eventSub = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(sourceRunId),
            _ => { try { wakeup.Release(); } catch (Exception ex) when (ex is SemaphoreFullException or ObjectDisposedException) { } return Task.CompletedTask; },
            ct);

        long lastSeenId = 0;

        while (!ct.IsCancellationRequested)
        {
            var events = await store.GetEventsAsync(sourceRunId, lastSeenId,
                types: [RunEventType.Output, RunEventType.OutputComplete], cancellationToken: ct);

            foreach (var evt in events)
            {
                lastSeenId = evt.Id;

                if (evt.EventType == RunEventType.OutputComplete)
                {
                    using var doc = JsonDocument.Parse(evt.Payload);
                    if (doc.RootElement.TryGetProperty("error", out var errorProp) && errorProp.ValueKind == JsonValueKind.String)
                        throw new InvalidOperationException($"Stream source failed: {errorProp.GetString()}");
                    yield break;
                }

                yield return JsonSerializer.Deserialize<T>(evt.Payload, serializerOptions)!;
            }

            await wakeup.WaitAsync(TimeSpan.FromSeconds(5), ct);
        }

        ct.ThrowIfCancellationRequested();
    }
}
