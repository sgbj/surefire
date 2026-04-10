using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireMaintenanceService(
    IJobStore store,
    INotificationProvider notifications,
    JobRegistry registry,
    ActiveRunTracker activeRuns,
    SurefireOptions options,
    TimeProvider timeProvider,
    BatchCompletionHandler batchCompletionHandler,
    ILogger<SurefireMaintenanceService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.HeartbeatInterval, timeProvider);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await RunMaintenanceTickAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.MaintenanceTickFailed(logger, ex);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task RunMaintenanceTickAsync(CancellationToken cancellationToken)
    {
        var registrations = registry.Snapshot();

        foreach (var registered in registrations)
        {
            await store.UpsertJobAsync(registered.Definition, cancellationToken);
        }

        foreach (var queue in options.Queues)
        {
            await store.UpsertQueueAsync(queue, cancellationToken);
        }

        foreach (var rateLimit in options.RateLimits)
        {
            await store.UpsertRateLimitAsync(rateLimit, cancellationToken);
        }

        await store.HeartbeatAsync(
            options.NodeName,
            registry.GetJobNames(),
            registry.GetQueueNames(),
            activeRuns.Snapshot(),
            cancellationToken);

        await RecoverStaleRunningRunsAsync(cancellationToken);

        foreach (var registered in registrations)
        {
            await ContinuousRunSeeder.EnsureCapacityAsync(
                store,
                notifications,
                timeProvider,
                registered.Definition,
                cancellationToken);
        }

        var cancelledExpiredRunIds = await store.CancelExpiredRunsWithIdsAsync(cancellationToken);
        await PublishExpiredCancellationNotificationsAsync(cancelledExpiredRunIds, cancellationToken);

        await RecoverStuckBatchesAsync(cancellationToken);
    }

    private async Task PublishExpiredCancellationNotificationsAsync(IReadOnlyList<string> runIds,
        CancellationToken cancellationToken)
    {
        if (runIds.Count == 0)
        {
            return;
        }

        foreach (var runId in runIds)
        {
            await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, cancellationToken);

            var run = await store.GetRunAsync(runId, cancellationToken);
            if (run?.BatchId is { } batchId)
            {
                await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), runId,
                    cancellationToken);
            }
        }
    }

    private async Task RecoverStuckBatchesAsync(CancellationToken cancellationToken)
    {
        var batchIds = await store.GetCompletableBatchIdsAsync(cancellationToken);
        foreach (var batchId in batchIds)
        {
            await batchCompletionHandler.RecoverBatchAsync(batchId, cancellationToken);
        }
    }

    private async Task RecoverStaleRunningRunsAsync(CancellationToken cancellationToken)
    {
        var staleBefore = timeProvider.GetUtcNow() - options.InactiveThreshold;
        var retryPolicyCache = new Dictionary<string, RetryPolicy>(StringComparer.Ordinal);
        const int take = 1000;

        while (true)
        {
            // Always query from skip=0: recovered runs transition out of Running status,
            // so the result set naturally shrinks each iteration.
            var page = await store.GetRunsAsync(
                new()
                {
                    Status = JobStatus.Running,
                    LastHeartbeatBefore = staleBefore,
                    OrderBy = RunOrderBy.CreatedAt
                },
                0,
                take,
                cancellationToken);

            if (page.Items.Count == 0)
            {
                break;
            }

            foreach (var run in page.Items)
            {
                var retryPolicy = await GetRetryPolicyForStaleRecoveryAsync(run.JobName, retryPolicyCache,
                    cancellationToken);

                if (run.Attempt > retryPolicy.MaxRetries)
                {
                    var now = timeProvider.GetUtcNow();
                    var deadLetter = RunStatusTransition.RunningToFailed(
                        run.Id,
                        run.Attempt,
                        now,
                        run.NotBefore,
                        run.NodeName,
                        run.Progress,
                        run.Error ?? "Run became stale and exhausted retry policy.",
                        run.Result,
                        run.StartedAt,
                        run.LastHeartbeatAt);

                    if (!await store.TryTransitionRunAsync(deadLetter, cancellationToken))
                    {
                        continue;
                    }

                    await batchCompletionHandler.AppendFailureEventAsync(
                        run,
                        RunFailureEnvelope.FromMessage(
                            run.Attempt,
                            now,
                            "Maintenance",
                            "StaleRecovery",
                            run.Error ?? "Run became stale and exhausted retry policy."),
                        cancellationToken);

                    await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id,
                        cancellationToken);
                    await batchCompletionHandler.MaybeCompleteBatchAsync(run.BatchId, run.Id, JobStatus.Failed, "Maintenance", cancellationToken);
                    continue;
                }

                var retryNotBefore = timeProvider.GetUtcNow() + retryPolicy.GetDelay(run.Attempt);

                var retrying = RunStatusTransition.RunningToRetrying(
                    run.Id,
                    run.Attempt,
                    retryNotBefore,
                    run.NodeName,
                    run.Progress,
                    run.Error,
                    run.Result,
                    run.LastHeartbeatAt);

                if (!await store.TryTransitionRunAsync(retrying, cancellationToken))
                {
                    continue;
                }

                await batchCompletionHandler.AppendFailureEventAsync(
                    run,
                    RunFailureEnvelope.FromMessage(
                        run.Attempt,
                        timeProvider.GetUtcNow(),
                        "Maintenance",
                        "StaleRecovery",
                        "Run became stale and was recovered for retry."),
                    cancellationToken);

                var pending = RunStatusTransition.RetryingToPending(
                    run.Id,
                    run.Attempt,
                    retryNotBefore,
                    null,
                    run.Progress,
                    run.Error,
                    run.Result);

                if (!await store.TryTransitionRunAsync(pending, cancellationToken))
                {
                    continue;
                }

                await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);
            }
        }
    }

    private async Task<RetryPolicy> GetRetryPolicyForStaleRecoveryAsync(string jobName,
        Dictionary<string, RetryPolicy> retryPolicyCache, CancellationToken cancellationToken)
    {
        if (!retryPolicyCache.TryGetValue(jobName, out var retryPolicy))
        {
            var job = await store.GetJobAsync(jobName, cancellationToken);
            retryPolicy = job?.RetryPolicy ?? new();
            retryPolicyCache[jobName] = retryPolicy;
        }

        return retryPolicy;
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1301, Level = LogLevel.Error, Message = "Maintenance tick failed.")]
        public static partial void MaintenanceTickFailed(ILogger logger, Exception exception);
    }
}