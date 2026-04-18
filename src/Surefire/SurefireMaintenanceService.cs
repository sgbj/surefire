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
    SurefireInstrumentation instrumentation,
    BatchCompletionHandler batchCompletionHandler,
    Backoff backoff,
    ILogger<SurefireMaintenanceService> logger) : BackgroundService
{
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.HeartbeatInterval, timeProvider);

        try
        {
            var backoffAttempt = 0;
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await RunMaintenanceTickAsync(stoppingToken);
                    backoffAttempt = 0;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }

                    Log.MaintenanceTickFailed(logger, ex);
                    if (!store.IsTransientException(ex))
                    {
                        throw;
                    }

                    instrumentation.RecordStoreRetry("maintenance");
                    await Task.Delay(backoff.NextDelay(backoffAttempt++, BackoffInitial, BackoffMax), timeProvider,
                        stoppingToken);
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
            var run = await store.GetRunAsync(runId, cancellationToken);
            if (run is { })
            {
                await batchCompletionHandler.AppendFailureEventAsync(
                    run,
                    RunFailureEnvelope.FromMessage(
                        run.Attempt,
                        timeProvider.GetUtcNow(),
                        "Maintenance",
                        "ExpiredCancellation",
                        "Cancelled: run expired past its NotAfter deadline."),
                    cancellationToken);
            }

            await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunAvailable, runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, cancellationToken);

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
                    // Stale recovery dead-letter: detail lives on the AttemptFailure
                    // event (see `StaleRecovery` message below). Preserve any non-exception
                    // Reason the run already carried (e.g. set during a prior claim); don't
                    // synthesize one here.
                    var deadLetter = RunStatusTransition.RunningToFailed(
                        run.Id,
                        run.Attempt,
                        now,
                        run.NotBefore,
                        run.NodeName,
                        run.Progress,
                        run.Reason,
                        run.Result,
                        run.StartedAt,
                        run.LastHeartbeatAt);

                    var result = await store.TryTransitionRunAsync(deadLetter, cancellationToken);
                    if (!result.Transitioned)
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
                            "Run became stale and exhausted retry policy."),
                        cancellationToken);

                    await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id,
                        cancellationToken);
                    await notifications.PublishAsync(NotificationChannels.RunAvailable, run.Id, cancellationToken);
                    await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
                    if (run.BatchId is { } staleBatchId)
                    {
                        await notifications.PublishAsync(NotificationChannels.RunEvent(staleBatchId), run.Id,
                            cancellationToken);
                    }

                    if (result.BatchCompletion is { } bc)
                    {
                        await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId,
                            cancellationToken);
                    }

                    continue;
                }

                var retryNotBefore = timeProvider.GetUtcNow() + backoff.NextDelay(run.Attempt - 1,
                    retryPolicy.InitialDelay, retryPolicy.MaxDelay, retryPolicy.Jitter, retryPolicy.BackoffType);

                var failureEvent = batchCompletionHandler.CreateFailureEvent(
                    run,
                    RunFailureEnvelope.FromMessage(
                        run.Attempt,
                        timeProvider.GetUtcNow(),
                        "Maintenance",
                        "StaleRecovery",
                        "Run became stale and was recovered for retry."));

                var pending = RunStatusTransition.RunningToPending(
                    run.Id,
                    run.Attempt,
                    retryNotBefore,
                    run.Reason,
                    run.Result,
                    events: failureEvent is { } ? [failureEvent] : null);

                var pendingResult = await store.TryTransitionRunAsync(pending, cancellationToken);
                if (!pendingResult.Transitioned)
                {
                    continue;
                }

                await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
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