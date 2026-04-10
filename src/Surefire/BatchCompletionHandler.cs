using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class BatchCompletionHandler(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options,
    TimeProvider timeProvider,
    ILogger<BatchCompletionHandler> logger)
{
    public async Task MaybeCompleteBatchAsync(string? batchId, string? childRunId, JobStatus terminalStatus,
        string failureSource, CancellationToken cancellationToken)
    {
        if (batchId is null)
        {
            return;
        }

        if (await store.TryIncrementBatchProgressAsync(batchId, terminalStatus, cancellationToken) is not { } counters)
        {
            return;
        }

        await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), childRunId ?? batchId,
            cancellationToken);
        if (counters.Succeeded + counters.Failed + counters.Cancelled < counters.Total)
        {
            return;
        }

        var completedAt = timeProvider.GetUtcNow();
        var batchStatus = counters.Failed > 0 ? JobStatus.Failed
            : counters.Cancelled > 0 ? JobStatus.Cancelled
            : JobStatus.Succeeded;
        if (!await store.TryCompleteBatchAsync(batchId, batchStatus, completedAt, cancellationToken))
        {
            return;
        }

        if (counters.Failed > 0 || counters.Cancelled > 0)
        {
            Log.BatchCompletedWithFailures(logger, batchId, counters.Failed, counters.Cancelled);
        }

        await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId, cancellationToken);
    }

    public async Task AppendFailureEventAsync(RunRecord run, RunFailureEnvelope envelope,
        CancellationToken cancellationToken)
    {
        try
        {
            var payload = JsonSerializer.Serialize(envelope, options.SerializerOptions);

            await store.AppendEventsAsync(
            [
                new()
                {
                    RunId = run.Id,
                    EventType = RunEventType.AttemptFailure,
                    Payload = payload,
                    CreatedAt = timeProvider.GetUtcNow(),
                    Attempt = run.Attempt
                }
            ], cancellationToken);

            await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
            if (run.BatchId is { } batchId)
            {
                await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), run.Id, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            Log.FailedToAppendAttemptFailure(logger, ex, run.Id);
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1500, Level = LogLevel.Information,
            Message = "Batch '{BatchId}' completed with {FailedCount} failed and {CancelledCount} cancelled run(s).")]
        public static partial void BatchCompletedWithFailures(ILogger logger, string batchId, int failedCount, int cancelledCount);

        [LoggerMessage(EventId = 1501, Level = LogLevel.Warning,
            Message = "Failed to append attempt failure event for run '{RunId}'.")]
        public static partial void FailedToAppendAttemptFailure(ILogger logger, Exception exception, string runId);
    }
}

