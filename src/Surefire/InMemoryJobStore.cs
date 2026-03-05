using System.Collections.Concurrent;

namespace Surefire;

internal sealed class InMemoryJobStore(TimeProvider timeProvider) : IJobStore, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, JobDefinition> _jobs = new();
    private readonly ConcurrentDictionary<string, JobRun> _runs = new();
    private readonly ConcurrentDictionary<string, NodeInfo> _nodes = new();
    private readonly List<RunEvent> _events = [];
    private readonly Lock _eventLock = new();
    private readonly Lock _claimLock = new();
    private readonly Lock _deduplicationLock = new();
    private readonly HashSet<string> _deduplicationIds = new();
    private long _eventIdCounter;

    public Task MigrateAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        _jobs.AddOrUpdate(job.Name,
            // Add: defensive copy so the caller can't mutate the stored object
            _ => new JobDefinition
            {
                Name = job.Name,
                Description = job.Description,
                Tags = [.. job.Tags],
                CronExpression = job.CronExpression,
                Timeout = job.Timeout,
                MaxConcurrency = job.MaxConcurrency,
                RetryPolicy = job.RetryPolicy,
                IsEnabled = job.IsEnabled
            },
            // Update: preserve the stored IsEnabled state (user may have toggled it via dashboard)
            (_, existing) => new JobDefinition
            {
                Name = job.Name,
                Description = job.Description,
                Tags = [.. job.Tags],
                CronExpression = job.CronExpression,
                Timeout = job.Timeout,
                MaxConcurrency = job.MaxConcurrency,
                RetryPolicy = job.RetryPolicy,
                IsEnabled = existing.IsEnabled
            });
        return Task.CompletedTask;
    }

    public Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        _jobs.TryGetValue(name, out var job);
        return Task.FromResult(job);
    }

    public Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobFilter? filter = null, CancellationToken cancellationToken = default)
    {
        var query = _jobs.Values.AsEnumerable();

        if (filter?.Name is not null)
            query = query.Where(j => j.Name.Contains(filter.Name, StringComparison.OrdinalIgnoreCase));
        if (filter?.Tag is not null)
            query = query.Where(j => j.Tags.Any(t => t.Contains(filter.Tag, StringComparison.OrdinalIgnoreCase)));
        if (filter?.IsEnabled is not null)
            query = query.Where(j => j.IsEnabled == filter.IsEnabled);

        return Task.FromResult<IReadOnlyList<JobDefinition>>(query.ToList());
    }

    public Task RemoveJobAsync(string name, CancellationToken cancellationToken = default)
    {
        _jobs.TryRemove(name, out _);
        return Task.CompletedTask;
    }

    public Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        _jobs.AddOrUpdate(name,
            _ => throw new KeyNotFoundException($"Job '{name}' not found"),
            (_, existing) => new JobDefinition
            {
                Name = existing.Name,
                Description = existing.Description,
                Tags = [.. existing.Tags],
                CronExpression = existing.CronExpression,
                Timeout = existing.Timeout,
                MaxConcurrency = existing.MaxConcurrency,
                RetryPolicy = existing.RetryPolicy,
                IsEnabled = enabled
            });
        return Task.CompletedTask;
    }

    public Task CreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        lock (_deduplicationLock)
        {
            if (run.DeduplicationId is not null && !_deduplicationIds.Add(run.DeduplicationId))
                throw new InvalidOperationException($"Duplicate deduplication ID '{run.DeduplicationId}'");

            _runs[run.Id] = run;
        }
        return Task.CompletedTask;
    }

    public Task<bool> TryCreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        lock (_deduplicationLock)
        {
            if (run.DeduplicationId is not null && !_deduplicationIds.Add(run.DeduplicationId))
                return Task.FromResult(false);

            _runs[run.Id] = run;
            return Task.FromResult(true);
        }
    }

    public Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        _runs.TryGetValue(id, out var run);
        return Task.FromResult(run);
    }

    public Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default)
    {
        var query = _runs.Values.AsEnumerable();

        if (filter.JobName is not null)
            query = query.Where(r => r.JobName.Contains(filter.JobName, StringComparison.OrdinalIgnoreCase));

        if (filter.Status is not null)
            query = query.Where(r => r.Status == filter.Status);

        if (filter.NodeName is not null)
            query = query.Where(r => r.NodeName == filter.NodeName);

        if (filter.ParentRunId is not null)
            query = query.Where(r => r.ParentRunId == filter.ParentRunId);

        if (filter.RetryOfRunId is not null)
            query = query.Where(r => r.RetryOfRunId == filter.RetryOfRunId);

        if (filter.RerunOfRunId is not null)
            query = query.Where(r => r.RerunOfRunId == filter.RerunOfRunId);

        if (filter.CreatedAfter is not null)
            query = query.Where(r => r.CreatedAt >= filter.CreatedAfter);
        if (filter.CreatedBefore is not null)
            query = query.Where(r => r.CreatedAt <= filter.CreatedBefore);

        var all = query.ToList();
        var totalCount = all.Count;

        var ordered = filter.OrderBy switch
        {
            RunOrderBy.StartedAt => all.OrderByDescending(r => r.StartedAt),
            RunOrderBy.CompletedAt => all.OrderByDescending(r => r.CompletedAt),
            _ => all.OrderByDescending(r => r.CreatedAt)
        };

        var items = ordered.Skip(filter.Skip).Take(filter.Take).ToList();
        return Task.FromResult(new PagedResult<JobRun> { Items = items, TotalCount = totalCount });
    }

    public Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        _runs[run.Id] = run;
        return Task.CompletedTask;
    }

    public Task<bool> TryUpdateRunStatusAsync(JobRun run, JobStatus expectedStatus, CancellationToken cancellationToken = default)
    {
        lock (_claimLock)
        {
            if (!_runs.TryGetValue(run.Id, out var existing) || existing.Status != expectedStatus)
                return Task.FromResult(false);

            // Apply the update (run may be a different object than existing)
            existing.Status = run.Status;
            existing.Error = run.Error;
            existing.CompletedAt = run.CompletedAt;
            existing.CancelledAt = run.CancelledAt;
            return Task.FromResult(true);
        }
    }

    public Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, CancellationToken cancellationToken = default)
    {
        var jobNameSet = registeredJobNames as IReadOnlySet<string> ?? new HashSet<string>(registeredJobNames);
        lock (_claimLock)
        {
            // Order by NotBefore then CreatedAt for FIFO semantics
            foreach (var kvp in _runs.OrderBy(kvp => kvp.Value.NotBefore).ThenBy(kvp => kvp.Value.CreatedAt))
            {
                var run = kvp.Value;
                if (run.Status != JobStatus.Pending)
                    continue;

                if (!jobNameSet.Contains(run.JobName))
                    continue;

                if (run.NotBefore > timeProvider.GetUtcNow())
                    continue;

                if (_jobs.TryGetValue(run.JobName, out var jobDef) && jobDef.MaxConcurrency is not null)
                {
                    var runningCount = _runs.Values.Count(r => r.JobName == run.JobName && r.Status == JobStatus.Running);
                    if (runningCount >= jobDef.MaxConcurrency)
                        continue;
                }

                var now = timeProvider.GetUtcNow();
                run.Status = JobStatus.Running;
                run.NodeName = nodeName;
                run.StartedAt = now;
                run.LastHeartbeatAt = now;
                return Task.FromResult<JobRun?>(run);
            }
        }

        return Task.FromResult<JobRun?>(null);
    }

    public Task RegisterNodeAsync(NodeInfo node, CancellationToken cancellationToken = default)
    {
        _nodes[node.Name] = node;
        return Task.CompletedTask;
    }

    public Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, CancellationToken cancellationToken = default)
    {
        if (_nodes.TryGetValue(nodeName, out var node))
        {
            node.LastHeartbeatAt = timeProvider.GetUtcNow();
            node.RunningCount = _runs.Values.Count(r => r.NodeName == nodeName && r.Status == JobStatus.Running);
            node.RegisteredJobNames = [.. registeredJobNames];
        }
        return Task.CompletedTask;
    }

    public Task RemoveNodeAsync(string nodeName, CancellationToken cancellationToken = default)
    {
        _nodes.TryRemove(nodeName, out _);
        return Task.CompletedTask;
    }

    public Task<NodeInfo?> GetNodeAsync(string nodeName, CancellationToken cancellationToken = default)
    {
        _nodes.TryGetValue(nodeName, out var node);
        return Task.FromResult(node);
    }

    public Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<NodeInfo>>(_nodes.Values.ToList());

    // -- Run Events --

    public Task AppendEventAsync(RunEvent evt, CancellationToken cancellationToken = default)
    {
        lock (_eventLock)
        {
            evt.Id = ++_eventIdCounter;
            _events.Add(evt);
        }
        return Task.CompletedTask;
    }

    public Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        lock (_eventLock)
        {
            foreach (var evt in events)
            {
                evt.Id = ++_eventIdCounter;
                _events.Add(evt);
            }
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0, RunEventType[]? types = null, CancellationToken cancellationToken = default)
    {
        lock (_eventLock)
        {
            var query = _events.Where(e => e.RunId == runId && e.Id > sinceId);
            if (types is { Length: > 0 })
                query = query.Where(e => types.Contains(e.EventType));
            return Task.FromResult<IReadOnlyList<RunEvent>>(query.ToList());
        }
    }

    // -- Heartbeat & Stale Runs --

    public Task HeartbeatRunsAsync(IReadOnlyCollection<string> runIds, CancellationToken cancellationToken = default)
    {
        var now = timeProvider.GetUtcNow();
        foreach (var runId in runIds)
        {
            if (_runs.TryGetValue(runId, out var run) && run.Status == JobStatus.Running)
                run.LastHeartbeatAt = now;
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<JobRun>> GetStaleRunsAsync(TimeSpan threshold, CancellationToken cancellationToken = default)
    {
        var cutoff = timeProvider.GetUtcNow() - threshold;
        var stale = _runs.Values
            .Where(r => r.Status == JobStatus.Running && (r.LastHeartbeatAt is null || r.LastHeartbeatAt < cutoff))
            .ToList();
        return Task.FromResult<IReadOnlyList<JobRun>>(stale);
    }

    public Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60, CancellationToken cancellationToken = default)
    {
        var allRuns = _runs.Values.ToList();
        var runs = since is not null
            ? allRuns.Where(r => r.CreatedAt >= since.Value).ToList()
            : allRuns;

        var completedCount = runs.Count(r => r.Status == JobStatus.Completed);
        var totalFinished = runs.Count(r => r.Status is JobStatus.Completed or JobStatus.Failed);

        var timeline = BuildTimeline(runs, since, bucketMinutes);

        var stats = new DashboardStats
        {
            TotalJobs = _jobs.Count,
            TotalRuns = runs.Count,
            ActiveRuns = runs.Count(r => r.Status == JobStatus.Running),
            SuccessRate = totalFinished > 0 ? (double)completedCount / totalFinished * 100 : 0,
            NodeCount = _nodes.Count,
            RecentRuns = runs.OrderByDescending(r => r.CreatedAt).Take(10).ToList(),
            RunsByStatus = runs.GroupBy(r => r.Status.ToString())
                .ToDictionary(g => g.Key, g => g.Count()),
            Timeline = timeline
        };

        return Task.FromResult(stats);
    }

    private List<TimelineBucket> BuildTimeline(List<JobRun> runs, DateTimeOffset? since, int bucketMinutes)
    {
        if (since is null || bucketMinutes <= 0)
            return [];

        var now = timeProvider.GetUtcNow();
        var bucketSpanTicks = TimeSpan.FromMinutes(bucketMinutes).Ticks;

        var grouped = runs.GroupBy(r =>
        {
            var ticks = r.CreatedAt.UtcTicks;
            return ticks - (ticks % bucketSpanTicks);
        }).ToDictionary(g => g.Key, g => g.ToList());

        var startTicks = since.Value.UtcTicks;
        startTicks -= startTicks % bucketSpanTicks;
        var endTicks = now.UtcTicks;
        endTicks -= endTicks % bucketSpanTicks;

        var buckets = new List<TimelineBucket>();
        for (var t = startTicks; t <= endTicks; t += bucketSpanTicks)
        {
            var bucket = new TimelineBucket { Timestamp = new DateTimeOffset(t, TimeSpan.Zero) };
            if (grouped.TryGetValue(t, out var bucketRuns))
            {
                foreach (var r in bucketRuns)
                {
                    switch (r.Status)
                    {
                        case JobStatus.Completed: bucket.Completed++; break;
                        case JobStatus.Failed: bucket.Failed++; break;
                        case JobStatus.Pending: bucket.Pending++; break;
                        case JobStatus.Running: bucket.Running++; break;
                        case JobStatus.Cancelled: bucket.Cancelled++; break;
                        case JobStatus.DeadLetter: bucket.DeadLetter++; break;
                    }
                }
            }
            buckets.Add(bucket);
        }

        return buckets;
    }

    public Task<int> PurgeRunsAsync(DateTimeOffset completedBefore, CancellationToken cancellationToken = default)
    {
        var purged = 0;
        // Snapshot keys to avoid iterating while mutating
        var keys = _runs.Keys.ToArray();
        foreach (var key in keys)
        {
            // Hold _claimLock to prevent racing with ClaimRunAsync which transitions
            // Pending → Running. Without this lock, purge could read Status == Pending,
            // then ClaimRunAsync claims the run, then purge removes it.
            lock (_claimLock)
            {
                // Re-read current value under lock (not a stale enumerator snapshot)
                if (!_runs.TryGetValue(key, out var run))
                    continue;

                var shouldPurge = (run.Status.IsTerminal && run.CompletedAt is not null && run.CompletedAt < completedBefore)
                    || (run.Status == JobStatus.Pending && run.CreatedAt < completedBefore);
                if (!shouldPurge) continue;

                if (_runs.TryRemove(key, out _))
                {
                    lock (_eventLock)
                        _events.RemoveAll(e => e.RunId == key);
                    lock (_deduplicationLock)
                    {
                        if (run.DeduplicationId is not null)
                            _deduplicationIds.Remove(run.DeduplicationId);
                    }
                    purged++;
                }
            }
        }
        return Task.FromResult(purged);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
