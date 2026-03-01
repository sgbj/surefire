using System.Text.Json;

namespace Surefire;

internal sealed class JobClient(IJobStore store, INotificationProvider notifications, SurefireOptions options, TimeProvider timeProvider) : IJobClient
{
    public async Task<string> TriggerAsync(string jobName, object? args = null, DateTimeOffset? notBefore = null, CancellationToken cancellationToken = default)
    {
        var run = CreateRun(jobName, args);
        if (notBefore is not null)
            run.NotBefore = notBefore.Value;
        await store.CreateRunAsync(run, cancellationToken);
        await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);
        return run.Id;
    }

    public async Task CancelAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken)
            ?? throw new InvalidOperationException($"Run '{runId}' not found.");

        if (run.Status.IsTerminal)
            throw new InvalidOperationException($"Run '{runId}' is already in a terminal state.");

        var now = timeProvider.GetUtcNow();
        run.CancelledAt = now;
        if (run.Status == JobStatus.Pending)
        {
            run.Status = JobStatus.Cancelled;
            run.CompletedAt = now;
        }
        await store.UpdateRunAsync(run, cancellationToken);
        await notifications.PublishAsync(NotificationChannels.RunCancelled(runId), "", cancellationToken);
    }

    public async Task<string> RerunAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken)
            ?? throw new InvalidOperationException($"Run '{runId}' not found.");

        object? args = run.Arguments is not null
            ? JsonSerializer.Deserialize<object>(run.Arguments, options.SerializerOptions)
            : null;

        return await TriggerAsync(run.JobName, args, cancellationToken: cancellationToken);
    }

    public async Task<TResult> RunAsync<TResult>(string jobName, object? args = null, CancellationToken cancellationToken = default)
    {
        var runId = Guid.CreateVersion7().ToString("N");

        var tcs = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var reg = cancellationToken.Register(() => tcs.TrySetCanceled(cancellationToken));

        var sub = await notifications.SubscribeAsync(
            NotificationChannels.RunCompleted(runId),
            message =>
            {
                try
                {
                    using var doc = JsonDocument.Parse(message);
                    var status = (JobStatus)doc.RootElement.GetProperty("status").GetInt32();

                    // Ignore intermediate failures that will be retried
                    if (status == JobStatus.Failed &&
                        doc.RootElement.TryGetProperty("willRetry", out var wr) && wr.GetBoolean())
                        return Task.CompletedTask;

                    if (status == JobStatus.Completed)
                    {
                        var resultElement = doc.RootElement.GetProperty("result");
                        if (resultElement.ValueKind == JsonValueKind.Null)
                        {
                            tcs.TrySetResult(default!);
                        }
                        else
                        {
                            var resultJson = resultElement.GetString()!;
                            var result = JsonSerializer.Deserialize<TResult>(resultJson, options.SerializerOptions);
                            tcs.TrySetResult(result!);
                        }
                    }
                    else
                    {
                        var error = doc.RootElement.GetProperty("error").GetString();
                        tcs.TrySetException(new InvalidOperationException($"Job '{jobName}' failed: {error}"));
                    }
                }
                catch (Exception ex)
                {
                    tcs.TrySetException(ex);
                }
                return Task.CompletedTask;
            },
            cancellationToken);

        try
        {
            var run = CreateRun(jobName, args, runId);
            await store.CreateRunAsync(run, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);

            return await tcs.Task;
        }
        finally
        {
            await sub.DisposeAsync();
        }
    }

    private JobRun CreateRun(string jobName, object? args, string? id = null)
    {
        var now = timeProvider.GetUtcNow();
        return new JobRun
        {
            Id = id ?? Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Arguments = args is not null ? JsonSerializer.Serialize(args, options.SerializerOptions) : null,
            CreatedAt = now,
            NotBefore = now,
            ParentRunId = JobContext.Current.Value?.RunId
        };
    }
}
