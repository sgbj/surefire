using System.Collections.Concurrent;
using System.Text.Json;
using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireScheduler(
    IJobStore store,
    INotificationProvider notifications,
    JobExecutor executor,
    JobRegistry registry,
    TimeProvider timeProvider,
    SurefireOptions options,
    ILogger<SurefireScheduler> logger) : BackgroundService
{
    private readonly ConcurrentDictionary<string, Task> _executingTasks = new();
    private IAsyncDisposable? _runCreatedSub;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var nodeName = options.NodeName;

        using var wakeup = new SemaphoreSlim(0);

        _runCreatedSub = await notifications.SubscribeAsync(
            NotificationChannels.RunCreated,
            _ => { wakeup.Release(); return Task.CompletedTask; },
            stoppingToken);

        var heartbeatInterval = options.HeartbeatInterval;
        var pollingInterval = options.PollingInterval;
        var retentionCheckInterval = options.RetentionCheckInterval;
        var lastHeartbeat = timeProvider.GetUtcNow();
        var lastRetentionCheck = timeProvider.GetUtcNow();

        logger.LogInformation("Surefire scheduler started on node {NodeName}", nodeName);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await EnqueueCronJobsAsync(stoppingToken);
                    await ClaimAndExecuteAsync(nodeName, stoppingToken);

                    var now = timeProvider.GetUtcNow();

                    if (now - lastHeartbeat > heartbeatInterval)
                    {
                        await store.HeartbeatAsync(nodeName, registry.GetJobNames(), stoppingToken);
                        await PruneStaleNodesAsync(nodeName, stoppingToken);
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
                    try { await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken); }
                    catch (OperationCanceledException) { break; }
                }
            }
        }
        finally
        {
            if (_runCreatedSub is not null)
                await _runCreatedSub.DisposeAsync();

            if (!_executingTasks.IsEmpty)
            {
                logger.LogInformation("Waiting for {Count} executing tasks to complete (timeout: {Timeout}s)...",
                    _executingTasks.Count, options.ShutdownTimeout.TotalSeconds);

                var allDone = Task.WhenAll(_executingTasks.Values);
                if (await Task.WhenAny(allDone, Task.Delay(options.ShutdownTimeout)) != allDone)
                {
                    logger.LogWarning("{Count} tasks did not complete within shutdown timeout, marking as cancelled", _executingTasks.Count);
                    var now = timeProvider.GetUtcNow();
                    foreach (var runId in _executingTasks.Keys)
                    {
                        try
                        {
                            var run = await store.GetRunAsync(runId);
                            if (run is { Status.IsTerminal: false })
                            {
                                run.Status = JobStatus.Cancelled;
                                run.CancelledAt ??= now;
                                run.CompletedAt ??= now;
                                run.Error = "Cancelled due to application shutdown";
                                await store.UpdateRunAsync(run);

                                try
                                {
                                    var payload = JsonSerializer.Serialize(
                                        new { run.Status, run.Result, run.Error, WillRetry = false },
                                        options.SerializerOptions);
                                    await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), payload);
                                }
                                catch { /* Best-effort during shutdown */ }
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "Failed to mark run {RunId} as cancelled during shutdown", runId);
                        }
                    }
                }
            }
        }

        logger.LogInformation("Surefire scheduler stopped");
    }

    private async Task EnqueueCronJobsAsync(CancellationToken ct)
    {
        var registeredNames = registry.GetJobNames();
        var jobs = await store.GetJobsAsync(cancellationToken: ct);
        var now = timeProvider.GetUtcNow();

        foreach (var job in jobs)
        {
            if (!registeredNames.Contains(job.Name)) continue;
            if (job.CronExpression is null) continue;
            if (!job.IsEnabled) continue;

            var cron = CronExpression.Parse(job.CronExpression);
            var nextOccurrence = cron.GetNextOccurrence(now.AddMinutes(-1).UtcDateTime, true);

            if (nextOccurrence is null) continue;

            var scheduledAt = new DateTimeOffset(nextOccurrence.Value, TimeSpan.Zero);
            if (scheduledAt > now) continue;

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

    private async Task PruneStaleNodesAsync(string currentNodeName, CancellationToken ct)
    {
        var staleThreshold = timeProvider.GetUtcNow() - options.StaleNodeThreshold;
        var nodes = await store.GetNodesAsync(ct);

        var staleNodes = nodes.Where(n => n.Name != currentNodeName && n.LastHeartbeatAt < staleThreshold).ToList();
        if (staleNodes.Count == 0) return;

        foreach (var node in staleNodes)
        {
            // Step 1 — Recover orphaned runs
            var orphanedRuns = await store.GetRunsAsync(new RunFilter { NodeName = node.Name, Status = JobStatus.Running, Take = 1000 }, ct);
            var now = timeProvider.GetUtcNow();

            foreach (var run in orphanedRuns.Items)
            {
                var jobDef = await store.GetJobAsync(run.JobName, ct);
                var retryPolicy = jobDef?.RetryPolicy ?? new RetryPolicy();
                var canRetry = run.Attempt < retryPolicy.MaxAttempts;

                run.Status = canRetry ? JobStatus.Failed
                    : retryPolicy.MaxAttempts > 1 ? JobStatus.DeadLetter
                    : JobStatus.Failed;
                run.Error = $"Node '{node.Name}' went offline";
                run.CompletedAt = now;
                await store.UpdateRunAsync(run, ct);

                // Notify subscribers on this run's channel (SSE log streams, etc.)
                var completionPayload = System.Text.Json.JsonSerializer.Serialize(
                    new { run.Status, run.Result, run.Error, WillRetry = canRetry },
                    options.SerializerOptions);
                await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), completionPayload, ct);

                if (canRetry)
                {
                    var retryDelay = retryPolicy.GetDelay(run.Attempt);
                    var retryRun = new JobRun
                    {
                        Id = Guid.CreateVersion7().ToString("N"),
                        JobName = run.JobName,
                        Arguments = run.Arguments,
                        CreatedAt = now,
                        NotBefore = now + retryDelay,
                        Attempt = run.Attempt + 1,
                        OriginalRunId = run.OriginalRunId ?? run.Id
                    };
                    await store.CreateRunAsync(retryRun, ct);
                    await notifications.PublishAsync(NotificationChannels.RunCreated, retryRun.Id, ct);
                    logger.LogInformation("Created retry {Attempt} for orphaned run {RunId} on stale node {NodeName}",
                        retryRun.Attempt, run.Id, node.Name);
                }
                else if (run.OriginalRunId is not null)
                {
                    // Also notify the original run's channel for RunAsync waiters
                    await notifications.PublishAsync(NotificationChannels.RunCompleted(run.OriginalRunId), completionPayload, ct);
                }
            }

            // Step 2 — Clean up orphaned jobs
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
                    logger.LogInformation("Removed orphaned job {JobName} — no remaining nodes handle it", jobName);
                }
            }

            // Step 3 — Remove the node
            await store.RemoveNodeAsync(node.Name, ct);
            logger.LogInformation("Pruned stale node {NodeName}", node.Name);
        }
    }
}
