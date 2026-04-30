using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class BatchCompletionHandler(
    IJobStore store,
    INotificationProvider notifications,
    BatchedEventWriter eventWriter,
    TimeProvider timeProvider,
    ILogger<BatchCompletionHandler> logger)
{
    public async Task RecoverBatchAsync(string batchId, CancellationToken cancellationToken)
    {
        var batch = await store.GetBatchAsync(batchId, cancellationToken);
        if (batch is null || batch.Status.IsTerminal)
        {
            return;
        }

        if (batch.Succeeded + batch.Failed + batch.Canceled < batch.Total)
        {
            return;
        }

        var completedAt = timeProvider.GetUtcNow();
        var batchStatus = batch.Failed > 0 ? JobStatus.Failed
            : batch.Canceled > 0 ? JobStatus.Canceled
            : JobStatus.Succeeded;

        if (!await store.TryCompleteBatchAsync(batchId, batchStatus, completedAt, cancellationToken))
        {
            return;
        }

        Log.BatchRecovered(logger, batchId);
        await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId, cancellationToken);
    }

    public RunEvent? CreateFailureEvent(JobRun run, RunFailureEnvelope envelope)
    {
        try
        {
            var payload = JsonSerializer.Serialize(envelope, SurefireJsonContext.Default.RunFailureEnvelope);
            return new()
            {
                RunId = run.Id,
                EventType = RunEventType.AttemptFailure,
                Payload = payload,
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = run.Attempt
            };
        }
        catch (Exception ex)
        {
            Log.FailedToAppendAttemptFailure(logger, ex, run.Id);
            return null;
        }
    }

    public async Task AppendFailureEventAsync(JobRun run, RunFailureEnvelope envelope,
        CancellationToken cancellationToken)
    {
        try
        {
            var payload = JsonSerializer.Serialize(envelope, SurefireJsonContext.Default.RunFailureEnvelope);

            BatchedEventWriter.NotificationPublish[] channels = run.BatchId is { } batchId
                ?
                [
                    new(NotificationChannels.RunEvent(run.Id), run.Id),
                    new(NotificationChannels.RunEvent(batchId), run.Id)
                ]
                : [new(NotificationChannels.RunEvent(run.Id), run.Id)];

            await eventWriter.EnqueueAsync(
                new()
                {
                    RunId = run.Id,
                    EventType = RunEventType.AttemptFailure,
                    Payload = payload,
                    CreatedAt = timeProvider.GetUtcNow(),
                    Attempt = run.Attempt
                },
                channels,
                cancellationToken);
        }
        catch (Exception ex)
        {
            Log.FailedToAppendAttemptFailure(logger, ex, run.Id);
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1501, Level = LogLevel.Warning,
            Message = "Failed to append attempt failure event for run '{RunId}'.")]
        public static partial void FailedToAppendAttemptFailure(ILogger logger, Exception exception, string runId);

        [LoggerMessage(EventId = 1502, Level = LogLevel.Information,
            Message = "Recovered stuck batch '{BatchId}' during maintenance sweep.")]
        public static partial void BatchRecovered(ILogger logger, string batchId);
    }
}
