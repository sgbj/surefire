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
    public async Task MaybeCompleteBatchAsync(string? batchRunId, string? childRunId, bool failed,
        string failureSource, CancellationToken cancellationToken)
    {
        if (batchRunId is null)
        {
            return;
        }

        if (await store.TryIncrementBatchCounterAsync(batchRunId, failed, cancellationToken) is not { } counters)
        {
            return;
        }

        await notifications.PublishAsync(NotificationChannels.RunEvent(batchRunId), childRunId ?? batchRunId,
            cancellationToken);
        if (counters.Succeeded + counters.Failed < counters.Total)
        {
            return;
        }

        var coordinator = await store.GetRunAsync(batchRunId, cancellationToken);
        if (coordinator is null || coordinator.Status.IsTerminal)
        {
            return;
        }

        var coordinatorStartedAt = coordinator.StartedAt ??
                                   await GetBatchEarliestChildStartedAtAsync(batchRunId, cancellationToken);

        var transition = counters.Failed == 0
            ? RunStatusTransition.RunningToSucceeded(
                coordinator.Id,
                coordinator.Attempt,
                timeProvider.GetUtcNow(),
                coordinator.NotBefore,
                coordinator.NodeName,
                1,
                null,
                null,
                coordinatorStartedAt,
                timeProvider.GetUtcNow())
            : RunStatusTransition.RunningToFailed(
                coordinator.Id,
                coordinator.Attempt,
                timeProvider.GetUtcNow(),
                coordinator.NotBefore,
                coordinator.NodeName,
                1,
                $"Batch completed with {counters.Failed} failed run(s).",
                null,
                coordinatorStartedAt,
                timeProvider.GetUtcNow());

        if (await store.TryTransitionRunAsync(transition, cancellationToken))
        {
            if (counters.Failed > 0)
            {
                await AppendFailureEventAsync(
                    coordinator,
                    RunFailureEnvelope.FromMessage(
                        coordinator.Attempt,
                        timeProvider.GetUtcNow(),
                        failureSource,
                        "BatchChildFailures",
                        $"Batch completed with {counters.Failed} failed run(s)."),
                    cancellationToken);
            }

            await notifications.PublishAsync(NotificationChannels.RunTerminated(coordinator.Id), coordinator.Id,
                cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(coordinator.Id), coordinator.Id,
                cancellationToken);
        }
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
            if (run.ParentRunId is { } batchRunId)
            {
                await notifications.PublishAsync(NotificationChannels.RunEvent(batchRunId), run.Id, cancellationToken);
            }
        }
        catch (Exception ex)
        {
            Log.FailedToAppendAttemptFailure(logger, ex, run.Id);
        }
    }

    private async Task<DateTimeOffset?> GetBatchEarliestChildStartedAtAsync(string batchRunId,
        CancellationToken cancellationToken)
    {
        DateTimeOffset? earliest = null;
        var skip = 0;
        const int take = 200;

        while (true)
        {
            var page = await store.GetRunsAsync(
                new()
                {
                    ParentRunId = batchRunId,
                    OrderBy = RunOrderBy.CreatedAt
                },
                skip,
                take,
                cancellationToken);

            if (page.Items.Count == 0)
            {
                break;
            }

            foreach (var child in page.Items)
            {
                if (child.StartedAt is not { } startedAt)
                {
                    continue;
                }

                earliest = earliest is null || startedAt < earliest
                    ? startedAt
                    : earliest;
            }

            skip += page.Items.Count;
        }

        return earliest;
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1501, Level = LogLevel.Warning,
            Message = "Failed to append attempt failure event for run '{RunId}'.")]
        public static partial void FailedToAppendAttemptFailure(ILogger logger, Exception exception, string runId);
    }
}
