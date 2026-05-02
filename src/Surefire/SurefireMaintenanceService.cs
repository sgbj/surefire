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
    LoopHealthTracker loopHealth,
    BatchCompletionHandler batchCompletionHandler,
    RunCancellationCoordinator runCancellation,
    Backoff backoff,
    ILogger<SurefireMaintenanceService> logger) : BackgroundService
{
    internal const string LoopName = "maintenance";
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        loopHealth.Register(LoopName, options.HeartbeatInterval);
        using var timer = new PeriodicTimer(options.HeartbeatInterval, timeProvider);

        try
        {
            var backoffAttempt = 0;
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await RunMaintenanceTickAsync(stoppingToken);
                    loopHealth.RecordSuccess(LoopName);
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

                    // Never crash the host on a single bad tick. Operators see the failure via
                    // metrics/health-check while the host stays up.
                    Log.MaintenanceTickFailed(logger, ex);
                    instrumentation.RecordLoopError(LoopName);
                    loopHealth.RecordFailure(LoopName);
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

        await store.UpsertJobsAsync(
            registrations.Select(r => r.Definition).ToList(),
            cancellationToken);

        // UpsertQueuesAsync also refreshes last_heartbeat_at, doubling as the heartbeat that
        // keeps retention from sweeping queues this node serves. Include both configured queues
        // and registry-derived ones (e.g. the implicit "default") so every queue this node
        // claims from stays alive.
        var configuredQueueNames = new HashSet<string>(
            options.Queues.Select(q => q.Name), StringComparer.Ordinal);
        var queuesToUpsert = new List<QueueDefinition>(options.Queues);
        foreach (var name in registry.GetQueueNames())
        {
            if (configuredQueueNames.Contains(name))
            {
                continue;
            }

            queuesToUpsert.Add(new() { Name = name });
        }

        await store.UpsertQueuesAsync(queuesToUpsert, cancellationToken);
        await store.UpsertRateLimitsAsync(options.RateLimits.ToList(), cancellationToken);

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

        var expired = await store.CancelExpiredRunsWithIdsAsync(cancellationToken);
        await PublishExpiredCancellationNotificationsAsync(expired, cancellationToken);

        await RecoverStuckBatchesAsync(cancellationToken);
    }

    private async Task PublishExpiredCancellationNotificationsAsync(SubtreeCancellation expired,
        CancellationToken cancellationToken)
    {
        if (expired.Runs.Count == 0 && expired.CompletedBatches.Count == 0)
        {
            return;
        }

        foreach (var entry in expired.Runs)
        {
            var runId = entry.RunId;
            var run = await store.GetRunAsync(runId, cancellationToken);
            if (run is { })
            {
                await batchCompletionHandler.AppendFailureEventAsync(
                    run,
                    RunFailureEnvelope.FromMessage(
                        run.Attempt,
                        timeProvider.GetUtcNow(),
                        "maintenance",
                        "expired_cancellation",
                        "Canceled: run expired past its NotAfter deadline."),
                    cancellationToken);
            }

            await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunAvailable, runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(runId), runId, cancellationToken);

            if (entry.BatchId is { } batchId)
            {
                await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), runId,
                    cancellationToken);
            }
        }

        foreach (var batch in expired.CompletedBatches)
        {
            await notifications.PublishAsync(NotificationChannels.BatchTerminated(batch.BatchId), batch.BatchId,
                cancellationToken);
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
        const int take = 500;

        while (true)
        {
            // Returns IDs ordered oldest-heartbeat first. Each processed run transitions out
            // of Running, so the filter set shrinks monotonically; loop until fewer than `take`.
            var staleIds = await store.GetStaleRunningRunIdsAsync(staleBefore, take, cancellationToken);

            if (staleIds.Count == 0)
            {
                break;
            }

            var staleRuns = await store.GetRunsByIdsAsync(staleIds, cancellationToken);

            foreach (var run in staleRuns)
            {
                if (run.Status != JobStatus.Running)
                {
                    continue;
                }

                var retryPolicy = await GetRetryPolicyForStaleRecoveryAsync(run.JobName, retryPolicyCache,
                    cancellationToken);

                if (run.Attempt > retryPolicy.MaxRetries)
                {
                    var now = timeProvider.GetUtcNow();
                    // Detail lives on the AttemptFailure event below. Preserve any Reason the
                    // run already carried; don't synthesize one here.
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

                    // Tag with StaleRecovery so operators can split "node died and we cleaned
                    // up its work" from "handler kept failing".
                    instrumentation.RecordRunFailed(run.JobName, run.StartedAt, now,
                        DeadLetterReason.StaleRecovery);

                    await batchCompletionHandler.AppendFailureEventAsync(
                        run,
                        RunFailureEnvelope.FromMessage(
                            run.Attempt,
                            now,
                            "maintenance",
                            "stale_recovery",
                            "Run became stale and exhausted retry policy."),
                        cancellationToken);

                    await runCancellation.CancelDescendantsAsync(
                        run.Id,
                        cancellationToken,
                        $"Canceled because parent run '{run.Id}' failed.");

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
                        "maintenance",
                        "stale_recovery",
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

                await runCancellation.CancelDescendantsAsync(
                    run.Id,
                    cancellationToken,
                    $"Canceled because parent run '{run.Id}' attempt {run.Attempt} failed before retry.");

                await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
                await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);
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
