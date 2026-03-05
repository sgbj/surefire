using System.Collections.Concurrent;
using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireScheduler(
    IJobStore store,
    INotificationProvider notifications,
    JobExecutor executor,
    JobRegistry registry,
    RunRecovery recovery,
    TimeProvider timeProvider,
    SurefireOptions options,
    ILogger<SurefireScheduler> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<string, Task> _executingTasks = new();
    private readonly ConcurrentDictionary<string, (string Expression, CronExpression Parsed)> _cronCache = new();
    private IAsyncDisposable? _runCreatedSub;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var nodeName = options.NodeName;

        // maxCount: 1 prevents unbounded accumulation when many notifications arrive in quick succession
        using var wakeup = new SemaphoreSlim(0, 1);

        _runCreatedSub = await notifications.SubscribeAsync(
            NotificationChannels.RunCreated,
            _ => { try { wakeup.Release(); } catch (SemaphoreFullException) { } return Task.CompletedTask; },
            stoppingToken);

        var heartbeatInterval = options.HeartbeatInterval;
        var pollingInterval = options.PollingInterval;
        var retentionCheckInterval = options.RetentionCheckInterval;
        var lastHeartbeat = timeProvider.GetUtcNow();
        var lastRetentionCheck = timeProvider.GetUtcNow();
        var lastCronCheck = timeProvider.GetUtcNow();

        logger.LogInformation("Surefire scheduler started on node {NodeName}", nodeName);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await EnqueueCronJobsAsync(lastCronCheck, stoppingToken);
                    lastCronCheck = timeProvider.GetUtcNow();
                    await ClaimAndExecuteAsync(nodeName, stoppingToken);

                    var now = timeProvider.GetUtcNow();

                    if (now - lastHeartbeat > heartbeatInterval)
                    {
                        await store.HeartbeatAsync(nodeName, registry.GetJobNames(), stoppingToken);

                        // Heartbeat all currently executing runs
                        var runIds = _executingTasks.Keys.ToList();
                        if (runIds.Count > 0)
                            await store.HeartbeatRunsAsync(runIds, stoppingToken);

                        // Re-sync job definitions so deleted/stale entries are restored
                        await SyncJobsAsync(stoppingToken);

                        // Detect and recover stale runs
                        await RecoverStaleRunsAsync(stoppingToken);

                        await PruneStaleNodesAsync(nodeName, stoppingToken);
                        await RemoveOrphanedJobsAsync(stoppingToken);
                        lastHeartbeat = now;
                    }

                    if (options.RetentionPeriod is { } retention && now - lastRetentionCheck > retentionCheckInterval)
                    {
                        var cutoff = now - retention;
                        var purged = await store.PurgeRunsAsync(cutoff, stoppingToken);
                        if (purged > 0)
                            logger.LogDebug("Purged {Count} expired runs", purged);
                        lastRetentionCheck = now;
                    }

                    await wakeup.WaitAsync(pollingInterval, stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error in scheduler loop");
                    try { await Task.Delay(TimeSpan.FromSeconds(5), timeProvider, stoppingToken); }
                    catch (OperationCanceledException) { break; }
                }
            }
        }
        finally
        {
            var sub = Interlocked.Exchange(ref _runCreatedSub, null);
            if (sub is not null)
                await sub.DisposeAsync();

            if (!_executingTasks.IsEmpty)
            {
                // Capture a consistent snapshot of tasks before waiting
                var snapshot = _executingTasks.ToArray();

                logger.LogInformation("Waiting for {Count} executing tasks to complete (timeout: {Timeout}s)...",
                    snapshot.Length, options.ShutdownTimeout.TotalSeconds);

                var allDone = Task.WhenAll(snapshot.Select(kvp => kvp.Value));
                var completed = await Task.WhenAny(allDone, Task.Delay(options.ShutdownTimeout, timeProvider));

                if (completed == allDone)
                {
                    // All tasks finished within timeout. Observe faulted tasks.
                    try { await allDone; }
                    catch { /* Individual task failures are already handled by ExecuteRunAsync */ }
                }
                else
                {
                    // Timeout expired. Observe faults without blocking shutdown.
                    _ = allDone.ContinueWith(
                        static t => { _ = t.Exception; },
                        CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted,
                        TaskScheduler.Default);

                    var incomplete = snapshot.Where(kvp => !kvp.Value.IsCompleted).Select(kvp => kvp.Key).ToList();
                    logger.LogWarning("{Count} tasks did not complete within shutdown timeout — will be recovered on next startup via stale-run detection",
                        incomplete.Count);
                }
            }
        }

        logger.LogInformation("Surefire scheduler stopped");
    }

    private async Task EnqueueCronJobsAsync(DateTimeOffset since, CancellationToken ct)
    {
        var registeredNames = registry.GetJobNames();
        var jobs = await store.GetJobsAsync(new JobFilter { IsEnabled = true }, ct);
        var now = timeProvider.GetUtcNow();

        foreach (var job in jobs)
        {
            if (!registeredNames.Contains(job.Name)) continue;
            if (job.CronExpression is null) continue;

            (string Expression, CronExpression Parsed) cached;
            try
            {
                cached = _cronCache.AddOrUpdate(
                    job.Name,
                    _ => (job.CronExpression, CronExpression.Parse(job.CronExpression)),
                    (_, existing) => existing.Expression == job.CronExpression
                        ? existing
                        : (job.CronExpression, CronExpression.Parse(job.CronExpression)));
            }
            catch (CronFormatException ex)
            {
                logger.LogWarning(ex, "Skipping job {JobName}: invalid cron expression '{Expression}'",
                    job.Name, job.CronExpression);
                _cronCache.TryRemove(job.Name, out _);
                continue;
            }

            // Enumerate all occurrences in the [since, now] window to catch up after delays.
            // Deduplication via TryCreateRunAsync prevents double-fire regardless.
            var occurrence = cached.Parsed.GetNextOccurrence(since.UtcDateTime, false);
            var catchUpLimit = 10; // Cap to prevent flooding after extended outages

            while (occurrence is not null && catchUpLimit > 0)
            {
                var scheduledAt = new DateTimeOffset(occurrence.Value, TimeSpan.Zero);
                if (scheduledAt > now) break;

                var run = new JobRun
                {
                    Id = Guid.CreateVersion7().ToString("N"),
                    JobName = job.Name,
                    CreatedAt = now,
                    NotBefore = now,
                    DeduplicationId = $"{job.Name}:{scheduledAt:O}"
                };

                if (await store.TryCreateRunAsync(run, ct))
                {
                    await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, ct);
                    logger.LogDebug("Enqueued cron job {JobName}", job.Name);
                }

                occurrence = cached.Parsed.GetNextOccurrence(occurrence.Value, false);
                catchUpLimit--;
            }
        }
    }

    private async Task ClaimAndExecuteAsync(string nodeName, CancellationToken ct)
    {
        var jobNames = registry.GetJobNames();
        while (!ct.IsCancellationRequested)
        {
            var run = await store.ClaimRunAsync(nodeName, jobNames, ct);
            if (run is null) break;

            var runId = run.Id;
            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _executingTasks[runId] = tcs.Task;
            _ = ExecuteRunAsync(run, tcs, ct);
        }
    }

    private async Task ExecuteRunAsync(JobRun run, TaskCompletionSource tcs, CancellationToken ct)
    {
        try
        {
            await executor.ExecuteAsync(run, ct);
            tcs.TrySetResult();
        }
        catch (Exception ex)
        {
            tcs.TrySetException(ex);
        }
        finally
        {
            _executingTasks.TryRemove(run.Id, out _);
        }
    }

    private async Task RecoverStaleRunsAsync(CancellationToken ct)
    {
        var staleRuns = await store.GetStaleRunsAsync(options.StaleNodeThreshold, ct);
        if (staleRuns.Count == 0) return;

        foreach (var run in staleRuns)
        {
            // Don't recover runs we're currently executing
            if (_executingTasks.ContainsKey(run.Id)) continue;

            if (await recovery.TryRecoverRunAsync(run, $"Run heartbeat stale (node: {run.NodeName})", ct))
                logger.LogInformation("Recovered stale run {RunId} for job {JobName}", run.Id, run.JobName);
        }
    }

    private async Task SyncJobsAsync(CancellationToken ct)
    {
        foreach (var name in registry.GetJobNames())
        {
            var job = registry.Get(name);
            if (job is not null)
                await store.UpsertJobAsync(job.Definition, ct);
        }
    }

    private async Task RemoveOrphanedJobsAsync(CancellationToken ct)
    {
        var allJobs = await store.GetJobsAsync(cancellationToken: ct);
        var allNodes = await store.GetNodesAsync(ct);
        var coveredJobs = new HashSet<string>(
            allNodes.SelectMany(n => n.RegisteredJobNames));

        foreach (var job in allJobs)
        {
            if (!coveredJobs.Contains(job.Name))
            {
                await store.RemoveJobAsync(job.Name, ct);
                logger.LogInformation("Removed orphaned job {JobName} — no active nodes handle it", job.Name);
            }
        }
    }

    private async Task PruneStaleNodesAsync(string currentNodeName, CancellationToken ct)
    {
        var staleThreshold = timeProvider.GetUtcNow() - options.StaleNodeThreshold;
        var nodes = await store.GetNodesAsync(ct);

        var staleNodes = nodes.Where(n => n.Name != currentNodeName && n.LastHeartbeatAt < staleThreshold).ToList();
        if (staleNodes.Count == 0) return;

        foreach (var node in staleNodes)
        {
            // Re-check the node's heartbeat before acting — it may have come back online
            // between our initial query and now (TOCTOU protection).
            var freshNode = await store.GetNodeAsync(node.Name, ct);
            if (freshNode is null || freshNode.LastHeartbeatAt >= staleThreshold)
                continue;

            // Step 1 — Recover orphaned runs
            var orphanedRuns = await store.GetRunsAsync(new RunFilter { NodeName = node.Name, Status = JobStatus.Running, Take = 1000 }, ct);

            if (orphanedRuns.TotalCount > orphanedRuns.Items.Count)
                logger.LogWarning("Node {NodeName} has {Total} orphaned runs, processing first {Count}",
                    node.Name, orphanedRuns.TotalCount, orphanedRuns.Items.Count);

            foreach (var run in orphanedRuns.Items)
            {
                if (await recovery.TryRecoverRunAsync(run, $"Node '{node.Name}' went offline", ct))
                    logger.LogInformation("Recovered orphaned run {RunId} from stale node {NodeName}", run.Id, node.Name);
            }

            // Step 2 — Remove orphaned jobs (no active node handles them).
            // Safe because SyncJobsAsync re-creates them when a capable node starts.
            var remainingNodes = await store.GetNodesAsync(ct);
            var coveredJobs = new HashSet<string>(
                remainingNodes
                    .Where(n => n.Name != node.Name)
                    .SelectMany(n => n.RegisteredJobNames));

            foreach (var jobName in node.RegisteredJobNames)
            {
                if (!coveredJobs.Contains(jobName))
                {
                    await store.RemoveJobAsync(jobName, ct);
                    logger.LogInformation("Removed orphaned job {JobName} — no active nodes handle it", jobName);
                }
            }

            // Step 3 — Remove the node
            await store.RemoveNodeAsync(node.Name, ct);
            logger.LogInformation("Pruned stale node {NodeName}", node.Name);
        }
    }
}
