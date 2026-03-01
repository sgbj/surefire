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
    ILogger<SurefireMaintenanceService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.HeartbeatInterval);

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
            await EnsureContinuousSeedCapacityAsync(registered.Definition, cancellationToken);
        }

        await store.CancelExpiredRunsAsync(cancellationToken);
    }

    private async Task RecoverStaleRunningRunsAsync(CancellationToken cancellationToken)
    {
        var staleBefore = timeProvider.GetUtcNow() - options.InactiveThreshold;
        var retryPolicyCache = new Dictionary<string, int>(StringComparer.Ordinal);

        while (true)
        {
            var page = await store.GetRunsAsync(
                new()
                {
                    Status = JobStatus.Running,
                    LastHeartbeatBefore = staleBefore,
                    IsBatchCoordinator = false,
                    OrderBy = RunOrderBy.CreatedAt
                },
                0,
                1000,
                cancellationToken);

            if (page.Items.Count == 0)
            {
                break;
            }

            foreach (var run in page.Items)
            {
                if (!await CanRetryAfterStaleRecoveryAsync(run.JobName, run.Attempt, retryPolicyCache,
                        cancellationToken))
                {
                    var now = timeProvider.GetUtcNow();
                    var deadLetter = RunStatusTransition.RunningToDeadLetter(
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

                    await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), run.Id,
                        cancellationToken);
                    await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
                    await MaybeCompleteBatchAsync(run.ParentRunId, run.Id, cancellationToken);
                    continue;
                }

                var retrying = RunStatusTransition.RunningToRetrying(
                    run.Id,
                    run.Attempt,
                    timeProvider.GetUtcNow(),
                    run.NodeName,
                    run.Progress,
                    run.Error,
                    run.Result,
                    run.LastHeartbeatAt);

                if (!await store.TryTransitionRunAsync(retrying, cancellationToken))
                {
                    continue;
                }

                var pending = RunStatusTransition.RetryingToPending(
                    run.Id,
                    run.Attempt,
                    timeProvider.GetUtcNow(),
                    null,
                    run.Progress,
                    run.Error,
                    run.Result);

                if (!await store.TryTransitionRunAsync(pending, cancellationToken))
                {
                    continue;
                }

                await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);
                await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
            }
        }
    }

    private async Task<bool> CanRetryAfterStaleRecoveryAsync(string jobName, int currentAttempt,
        Dictionary<string, int> retryPolicyCache, CancellationToken cancellationToken)
    {
        if (!retryPolicyCache.TryGetValue(jobName, out var maxRetries))
        {
            var job = await store.GetJobAsync(jobName, cancellationToken);
            maxRetries = job?.RetryPolicy.MaxRetries ?? 0;
            retryPolicyCache[jobName] = maxRetries;
        }

        return currentAttempt <= maxRetries;
    }

    private async Task MaybeCompleteBatchAsync(string? batchRunId, string childRunId,
        CancellationToken cancellationToken)
    {
        if (batchRunId is null)
        {
            return;
        }

        var counters = await store.IncrementBatchCounterAsync(batchRunId, true, cancellationToken);
        await notifications.PublishAsync(NotificationChannels.RunEvent(batchRunId), childRunId, cancellationToken);

        if (counters.Completed + counters.Failed < counters.Total)
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

        var now = timeProvider.GetUtcNow();
        var transition = RunStatusTransition.RunningToDeadLetter(
            coordinator.Id,
            coordinator.Attempt,
            now,
            coordinator.NotBefore,
            coordinator.NodeName,
            1,
            $"Batch completed with {counters.Failed} failed run(s).",
            null,
            coordinatorStartedAt,
            now);

        if (await store.TryTransitionRunAsync(transition, cancellationToken))
        {
            await notifications.PublishAsync(NotificationChannels.RunCompleted(coordinator.Id), coordinator.Id,
                cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(coordinator.Id), coordinator.Id,
                cancellationToken);
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

    private async Task EnsureContinuousSeedCapacityAsync(JobDefinition definition, CancellationToken cancellationToken)
    {
        if (!definition.IsContinuous || !definition.IsEnabled)
        {
            return;
        }

        var desired = Math.Max(definition.MaxConcurrency ?? 1, 1);
        for (var i = 0; i < desired; i++)
        {
            var now = timeProvider.GetUtcNow();
            var run = new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = definition.Name,
                Status = JobStatus.Pending,
                CreatedAt = now,
                NotBefore = now,
                Priority = definition.Priority,
                QueuePriority = 0,
                Progress = 0,
                Attempt = 0
            };

            var created = await store.TryCreateRunAsync(
                run,
                desired,
                cancellationToken: cancellationToken);
            if (!created)
            {
                break;
            }

            await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1301, Level = LogLevel.Error, Message = "Maintenance tick failed.")]
        public static partial void MaintenanceTickFailed(ILogger logger, Exception exception);
    }
}