using System.Text.Json;

namespace Surefire;

/// <summary>
/// An IAsyncEnumerable&lt;T&gt; that reads Input events from the event store for a specific parameter.
/// Used on the executor side when a job declares an IAsyncEnumerable&lt;T&gt; parameter.
/// When sourceRunId differs from runId (retries/reruns), events are read from the source
/// and copied to the current run, making each run self-contained.
/// </summary>
internal sealed class RunEventInputEnumerable<T>(
    string runId,
    string? sourceRunId,
    string parameterName,
    IJobStore store,
    INotificationProvider notifications,
    CancellationToken cancellationToken,
    JsonSerializerOptions serializerOptions) : IAsyncEnumerable<T>
{
    public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken token = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, token);
        var ct = linkedCts.Token;

        var readFromRunId = sourceRunId ?? runId;

        using var wakeup = new SemaphoreSlim(0, 1);

        await using var inputSub = await notifications.SubscribeAsync(
            NotificationChannels.RunInput(readFromRunId),
            _ => { try { wakeup.Release(); } catch (Exception ex) when (ex is SemaphoreFullException or ObjectDisposedException) { } return Task.CompletedTask; },
            ct);

        long lastSeenId = 0;

        while (!ct.IsCancellationRequested)
        {
            var events = await store.GetEventsAsync(readFromRunId, lastSeenId,
                types: [RunEventType.Input, RunEventType.InputComplete], cancellationToken: ct);

            foreach (var evt in events)
            {
                lastSeenId = evt.Id;

                using var doc = JsonDocument.Parse(evt.Payload);
                var param = doc.RootElement.GetProperty("param").GetString();
                if (!string.Equals(param, parameterName, StringComparison.OrdinalIgnoreCase))
                    continue;

                // Copy to own run if reading from a source run (retries/reruns)
                if (readFromRunId != runId)
                {
                    var copy = new RunEvent
                    {
                        RunId = runId,
                        EventType = evt.EventType,
                        Payload = evt.Payload,
                        CreatedAt = evt.CreatedAt
                    };
                    await store.AppendEventAsync(copy, ct);
                    await notifications.PublishAsync(NotificationChannels.RunInput(runId), "", ct);
                }

                if (evt.EventType == RunEventType.InputComplete)
                {
                    if (doc.RootElement.TryGetProperty("cancelled", out var cancelledProp) && cancelledProp.GetBoolean())
                        throw new ChildRunCancelledException(runId);
                    if (doc.RootElement.TryGetProperty("error", out var errorProp))
                        throw new InvalidOperationException($"Input stream '{parameterName}' failed: {errorProp.GetString()}");
                    yield break;
                }

                var value = doc.RootElement.GetProperty("value");
                yield return JsonSerializer.Deserialize<T>(value.GetRawText(), serializerOptions)!;
            }

            await wakeup.WaitAsync(TimeSpan.FromSeconds(5), ct);
        }

        ct.ThrowIfCancellationRequested();
    }
}
