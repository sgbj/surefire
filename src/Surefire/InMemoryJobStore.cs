using System.Collections.Concurrent;

namespace Surefire;

internal sealed class InMemoryJobStore(TimeProvider timeProvider) : IJobStore, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, JobDefinition> _jobs = new();
    private readonly ConcurrentDictionary<string, JobRun> _runs = new();
    private readonly ConcurrentDictionary<string, NodeInfo> _nodes = new();
    private readonly ConcurrentDictionary<string, List<RunLogEntry>> _logs = new();
    private readonly Lock _logLock = new();
    private readonly Lock _claimLock = new();
    private readonly Lock _deduplicationLock = new();
    private readonly HashSet<string> _deduplicationIds = new();

    public Task MigrateAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        _jobs.AddOrUpdate(job.Name, job, (_, existing) =>
        {
            job.IsEnabled = existing.IsEnabled;
            return job;
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
        if (_jobs.TryGetValue(name, out var job))
            job.IsEnabled = enabled;
        return Task.CompletedTask;
    }

    public Task CreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        _runs[run.Id] = run;
        if (run.DeduplicationId is not null)
        {
            lock (_deduplicationLock)
                _deduplicationIds.Add(run.DeduplicationId);
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

        if (filter.OriginalRunId is not null)
            query = query.Where(r => r.OriginalRunId == filter.OriginalRunId);

        if (filter.CreatedAfter is not null)
            query = query.Where(r => r.CreatedAt >= filter.CreatedAfter);
        if (filter.CreatedBefore is not null)
            query = query.Where(r => r.CreatedAt <= filter.CreatedBefore);

        var all = query.ToList();
        var totalCount = all.Count;

        var ordered = filter.OrderBy switch
        {
            "CreatedAt" => all.OrderByDescending(r => r.CreatedAt),
            "StartedAt" => all.OrderByDescending(r => r.StartedAt),
            "CompletedAt" => all.OrderByDescending(r => r.CompletedAt),
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

    public Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, CancellationToken cancellationToken = default)
    {
        lock (_claimLock)
        {
            foreach (var kvp in _runs)
            {
                var run = kvp.Value;
                if (run.Status != JobStatus.Pending)
                    continue;

                if (!registeredJobNames.Contains(run.JobName))
                    continue;

                if (run.NotBefore > timeProvider.GetUtcNow())
                    continue;

                if (_jobs.TryGetValue(run.JobName, out var jobDef) && jobDef.MaxConcurrency is not null)
                {
                    var runningCount = _runs.Values.Count(r => r.JobName == run.JobName && r.Status == JobStatus.Running);
                    if (runningCount >= jobDef.MaxConcurrency)
                        continue;
                }

                run.Status = JobStatus.Running;
                run.NodeName = nodeName;
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

    public Task AddRunLogsAsync(IReadOnlyList<RunLogEntry> entries, CancellationToken cancellationToken = default)
    {
        lock (_logLock)
        {
            foreach (var group in entries.GroupBy(e => e.RunId))
            {
                if (!_logs.TryGetValue(group.Key, out var list))
                {
                    list = [];
                    _logs[group.Key] = list;
                }
                list.AddRange(group);
            }
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<RunLogEntry>> GetRunLogsAsync(string runId, CancellationToken cancellationToken = default)
    {
        lock (_logLock)
        {
            if (_logs.TryGetValue(runId, out var list))
                return Task.FromResult<IReadOnlyList<RunLogEntry>>(list.ToList());
        }
        return Task.FromResult<IReadOnlyList<RunLogEntry>>([]);
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
        if (since is null)
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
        foreach (var kvp in _runs)
        {
            var run = kvp.Value;
            var shouldPurge = (run.Status.IsTerminal && run.CompletedAt is not null && run.CompletedAt < completedBefore)
                || (run.Status == JobStatus.Pending && run.CreatedAt < completedBefore);
            if (shouldPurge)
            {
                if (_runs.TryRemove(kvp.Key, out _))
                {
                    _logs.TryRemove(kvp.Key, out _);
                    if (run.DeduplicationId is not null)
                    {
                        lock (_deduplicationLock)
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
