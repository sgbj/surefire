using System.Collections.Concurrent;
using System.Threading.RateLimiting;

namespace Surefire;

internal sealed class InMemoryJobStore : IJobStore, IAsyncDisposable
{
    private readonly TimeProvider _timeProvider;
    private readonly ConcurrentDictionary<string, JobDefinition> _jobs = new();
    private readonly ConcurrentDictionary<string, JobRun> _runs = new();
    private readonly ConcurrentDictionary<string, NodeInfo> _nodes = new();
    private readonly ConcurrentDictionary<string, QueueDefinition> _queues = new();
    private readonly ConcurrentDictionary<string, RateLimitDefinition> _rateLimitDefinitions = new();
    private readonly PartitionedRateLimiter<string> _rateLimiter;
    private readonly List<RunEvent> _events = [];
    private readonly Lock _eventLock = new();
    private readonly Lock _claimLock = new();
    private readonly Lock _deduplicationLock = new();
    private readonly HashSet<string> _deduplicationIds = new();
    private long _eventIdCounter;

    public InMemoryJobStore(TimeProvider timeProvider)
    {
        _timeProvider = timeProvider;
        _rateLimiter = PartitionedRateLimiter.Create<string, string>(name =>
        {
            if (!_rateLimitDefinitions.TryGetValue(name, out var def))
                return RateLimitPartition.GetNoLimiter(name);

            return def.Type switch
            {
                RateLimitType.SlidingWindow => RateLimitPartition.GetSlidingWindowLimiter(name, _ =>
                    new SlidingWindowRateLimiterOptions
                    {
                        PermitLimit = def.MaxPermits,
                        Window = def.Window,
                        SegmentsPerWindow = 2,
                        AutoReplenishment = true
                    }),
                _ => RateLimitPartition.GetFixedWindowLimiter(name, _ =>
                    new FixedWindowRateLimiterOptions
                    {
                        PermitLimit = def.MaxPermits,
                        Window = def.Window,
                        AutoReplenishment = true
                    })
            };
        });
    }

    public Task MigrateAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    // -- Jobs --

    public Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        _jobs.AddOrUpdate(job.Name,
            // Add: defensive copy so the caller can't mutate the stored object
            _ => new JobDefinition
            {
                Name = job.Name,
                Description = job.Description,
                Tags = [.. job.Tags],
                CronExpression = job.CronExpression,
                TimeZoneId = job.TimeZoneId,
                Timeout = job.Timeout,
                MaxConcurrency = job.MaxConcurrency,
                Priority = job.Priority,
                RetryPolicy = job.RetryPolicy,
                IsContinuous = job.IsContinuous,
                Queue = job.Queue,
                RateLimitName = job.RateLimitName,
                IsEnabled = job.IsEnabled,
                IsPlan = job.IsPlan,
                IsInternal = job.IsInternal,
                PlanGraph = job.PlanGraph,
                MisfirePolicy = job.MisfirePolicy,
                ArgumentsSchema = job.ArgumentsSchema,
                LastHeartbeatAt = now
            },
            // Update: preserve the stored IsEnabled state (user may have toggled it via dashboard)
            (_, existing) => new JobDefinition
            {
                Name = job.Name,
                Description = job.Description,
                Tags = [.. job.Tags],
                CronExpression = job.CronExpression,
                TimeZoneId = job.TimeZoneId,
                Timeout = job.Timeout,
                MaxConcurrency = job.MaxConcurrency,
                Priority = job.Priority,
                RetryPolicy = job.RetryPolicy,
                IsContinuous = job.IsContinuous,
                Queue = job.Queue,
                RateLimitName = job.RateLimitName,
                IsEnabled = existing.IsEnabled,
                IsPlan = job.IsPlan,
                IsInternal = job.IsInternal,
                PlanGraph = job.PlanGraph,
                MisfirePolicy = job.MisfirePolicy,
                ArgumentsSchema = job.ArgumentsSchema,
                LastHeartbeatAt = now
            });
        return Task.CompletedTask;
    }

    public Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        _jobs.TryGetValue(name, out var job);
        return Task.FromResult(job);
    }

    public Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null, CancellationToken cancellationToken = default)
    {
        var query = _jobs.Values.AsEnumerable();

        if (filter?.Name is not null)
            query = query.Where(j => j.Name.Contains(filter.Name, StringComparison.OrdinalIgnoreCase));
        if (filter?.Tag is not null)
            query = query.Where(j => j.Tags.Any(t => t.Contains(filter.Tag, StringComparison.OrdinalIgnoreCase)));
        if (filter?.IsEnabled is not null)
            query = query.Where(j => j.IsEnabled == filter.IsEnabled);
        if (filter?.HeartbeatAfter is not null)
            query = query.Where(j => j.LastHeartbeatAt is not null && j.LastHeartbeatAt >= filter.HeartbeatAfter);
        if (filter?.IncludeInternal != true)
            query = query.Where(j => !j.IsInternal);

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
                TimeZoneId = existing.TimeZoneId,
                Timeout = existing.Timeout,
                MaxConcurrency = existing.MaxConcurrency,
                Priority = existing.Priority,
                RetryPolicy = existing.RetryPolicy,
                IsContinuous = existing.IsContinuous,
                Queue = existing.Queue,
                RateLimitName = existing.RateLimitName,
                IsEnabled = enabled,
                IsPlan = existing.IsPlan,
                IsInternal = existing.IsInternal,
                PlanGraph = existing.PlanGraph,
                MisfirePolicy = existing.MisfirePolicy,
                ArgumentsSchema = existing.ArgumentsSchema,
                LastHeartbeatAt = existing.LastHeartbeatAt
            });
        return Task.CompletedTask;
    }

    // -- Runs --

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

    public Task CreateRunsAsync(IReadOnlyList<JobRun> runs, CancellationToken cancellationToken = default)
    {
        foreach (var run in runs)
            CreateRunAsync(run, cancellationToken);
        return Task.CompletedTask;
    }

    public Task<bool> TryCreateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        lock (_deduplicationLock)
        {
            if (run.DeduplicationId is not null && !_deduplicationIds.Add(run.DeduplicationId))
                return Task.FromResult(false);

            // Check plan step uniqueness: only one non-retry run per (PlanRunId, PlanStepId)
            if (run.PlanRunId is not null && run.PlanStepId is not null && run.RetryOfRunId is null)
            {
                var exists = _runs.Values.Any(r =>
                    r.PlanRunId == run.PlanRunId &&
                    r.PlanStepId == run.PlanStepId &&
                    r.RetryOfRunId is null);
                if (exists)
                    return Task.FromResult(false);
            }

            _runs[run.Id] = run;
            return Task.FromResult(true);
        }
    }

    public Task<bool> TryCreateContinuousRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        lock (_claimLock)
        {
            // Check job is enabled
            if (_jobs.TryGetValue(run.JobName, out var jobDef) && !jobDef.IsEnabled)
                return Task.FromResult(false);

            // Check no pending or running run exists for this job beyond max concurrency
            var activeCount = _runs.Values.Count(r =>
                r.JobName == run.JobName &&
                r.Status is JobStatus.Pending or JobStatus.Running);
            var maxConcurrency = jobDef?.MaxConcurrency ?? 1;
            if (activeCount >= maxConcurrency)
                return Task.FromResult(false);

            // Dedup check
            lock (_deduplicationLock)
            {
                if (run.DeduplicationId is not null && !_deduplicationIds.Add(run.DeduplicationId))
                    return Task.FromResult(false);

                _runs[run.Id] = run;
            }
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
            query = filter.ExactJobName
                ? query.Where(r => r.JobName == filter.JobName)
                : query.Where(r => r.JobName.Contains(filter.JobName, StringComparison.OrdinalIgnoreCase));

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

        if (filter.PlanRunId is not null)
            query = query.Where(r => r.PlanRunId == filter.PlanRunId);

        if (filter.PlanStepName is not null)
            query = query.Where(r => r.PlanStepName == filter.PlanStepName);

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
            existing.Result = run.Result;
            existing.Progress = run.Progress;
            existing.CompletedAt = run.CompletedAt;
            existing.CancelledAt = run.CancelledAt;
            return Task.FromResult(true);
        }
    }

    public Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default)
    {
        if (registeredJobNames.Count == 0)
            return Task.FromResult<JobRun?>(null);

        var jobNameSet = registeredJobNames as IReadOnlySet<string> ?? new HashSet<string>(registeredJobNames);
        var queueNameSet = queueNames as IReadOnlySet<string> ?? new HashSet<string>(queueNames);

        lock (_claimLock)
        {
            // Order by queue priority (descending), then job priority (descending), NotBefore, CreatedAt
            foreach (var kvp in _runs
                .OrderByDescending(kvp =>
                {
                    if (_jobs.TryGetValue(kvp.Value.JobName, out var j))
                    {
                        var qName = j.Queue ?? "default";
                        if (_queues.TryGetValue(qName, out var q))
                            return q.Priority;
                    }
                    return 0;
                })
                .ThenByDescending(kvp => kvp.Value.Priority)
                .ThenBy(kvp => kvp.Value.NotBefore)
                .ThenBy(kvp => kvp.Value.CreatedAt))
            {
                var run = kvp.Value;
                if (run.Status != JobStatus.Pending)
                    continue;

                if (!jobNameSet.Contains(run.JobName))
                    continue;

                if (run.NotBefore > _timeProvider.GetUtcNow())
                    continue;

                // Skip expired runs (will be cancelled by CancelExpiredRunsAsync)
                if (run.NotAfter is not null && run.NotAfter < _timeProvider.GetUtcNow())
                    continue;

                // Queue filtering
                var jobQueue = "default";
                if (_jobs.TryGetValue(run.JobName, out var jobDef))
                    jobQueue = jobDef.Queue ?? "default";

                if (!queueNameSet.Contains(jobQueue))
                    continue;

                // Check queue not paused
                if (_queues.TryGetValue(jobQueue, out var queueDef) && queueDef.IsPaused)
                    continue;

                // Max concurrency (job-level)
                if (jobDef?.MaxConcurrency is not null)
                {
                    var runningCount = _runs.Values.Count(r => r.JobName == run.JobName && r.Status == JobStatus.Running);
                    if (runningCount >= jobDef.MaxConcurrency)
                        continue;
                }

                // Max concurrency (queue-level)
                if (queueDef?.MaxConcurrency is not null)
                {
                    var queueRunningCount = _runs.Values.Count(r =>
                    {
                        if (r.Status != JobStatus.Running) return false;
                        var rQueue = "default";
                        if (_jobs.TryGetValue(r.JobName, out var rJob))
                            rQueue = rJob.Queue ?? "default";
                        return rQueue == jobQueue;
                    });
                    if (queueRunningCount >= queueDef.MaxConcurrency)
                        continue;
                }

                // Rate limiting
                var jobRateLimitName = jobDef?.RateLimitName;
                var queueRateLimitName = queueDef?.RateLimitName;

                // Try to acquire job-level rate limit
                RateLimitLease? jobLease = null;
                RateLimitLease? queueLease = null;

                if (jobRateLimitName is not null && _rateLimitDefinitions.ContainsKey(jobRateLimitName))
                {
                    jobLease = _rateLimiter.AttemptAcquire(jobRateLimitName);
                    if (!jobLease.IsAcquired)
                    {
                        jobLease.Dispose();
                        continue;
                    }
                }

                // Try to acquire queue-level rate limit (skip if same name as job rate limit)
                if (queueRateLimitName is not null &&
                    queueRateLimitName != jobRateLimitName &&
                    _rateLimitDefinitions.ContainsKey(queueRateLimitName))
                {
                    queueLease = _rateLimiter.AttemptAcquire(queueRateLimitName);
                    if (!queueLease.IsAcquired)
                    {
                        queueLease.Dispose();
                        jobLease?.Dispose();
                        continue;
                    }
                }

                var now = _timeProvider.GetUtcNow();
                run.Status = JobStatus.Running;
                run.NodeName = nodeName;
                run.StartedAt = now;
                run.LastHeartbeatAt = now;
                return Task.FromResult<JobRun?>(run);
            }
        }

        return Task.FromResult<JobRun?>(null);
    }

    // -- Rate Limits --

    public Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default)
    {
        _rateLimitDefinitions[rateLimit.Name] = rateLimit;
        return Task.CompletedTask;
    }

    public Task<bool> TryAcquireRateLimitAsync(string name, CancellationToken cancellationToken = default)
    {
        if (!_rateLimitDefinitions.ContainsKey(name))
            return Task.FromResult(false);

        var lease = _rateLimiter.AttemptAcquire(name);
        var acquired = lease.IsAcquired;
        if (!acquired) lease.Dispose();
        return Task.FromResult(acquired);
    }

    // -- Queues --

    public Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        _queues.AddOrUpdate(queue.Name,
            _ => new QueueDefinition
            {
                Name = queue.Name,
                Priority = queue.Priority,
                MaxConcurrency = queue.MaxConcurrency,
                IsPaused = queue.IsPaused,
                RateLimitName = queue.RateLimitName,
                LastHeartbeatAt = now
            },
            (_, existing) => new QueueDefinition
            {
                Name = queue.Name,
                Priority = queue.Priority,
                MaxConcurrency = queue.MaxConcurrency,
                IsPaused = existing.IsPaused, // Preserve paused state
                RateLimitName = queue.RateLimitName,
                LastHeartbeatAt = now
            });
        return Task.CompletedTask;
    }

    public Task<QueueDefinition?> GetQueueAsync(string name, CancellationToken cancellationToken = default)
    {
        _queues.TryGetValue(name, out var queue);
        return Task.FromResult(queue);
    }

    public Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<QueueDefinition>>(_queues.Values.ToList());

    public Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken cancellationToken = default)
    {
        _queues.AddOrUpdate(name,
            _ => throw new KeyNotFoundException($"Queue '{name}' not found"),
            (_, existing) => new QueueDefinition
            {
                Name = existing.Name,
                Priority = existing.Priority,
                MaxConcurrency = existing.MaxConcurrency,
                IsPaused = isPaused,
                RateLimitName = existing.RateLimitName,
                LastHeartbeatAt = existing.LastHeartbeatAt
            });
        return Task.CompletedTask;
    }

    // -- Nodes --

    public Task RegisterNodeAsync(NodeInfo node, CancellationToken cancellationToken = default)
    {
        _nodes[node.Name] = node;
        return Task.CompletedTask;
    }

    public Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, IReadOnlyCollection<string> registeredQueueNames, CancellationToken cancellationToken = default)
    {
        if (_nodes.TryGetValue(nodeName, out var node))
        {
            node.LastHeartbeatAt = _timeProvider.GetUtcNow();
            node.RunningCount = _runs.Values.Count(r => r.NodeName == nodeName && r.Status == JobStatus.Running);
            node.RegisteredJobNames = [.. registeredJobNames];
            node.RegisteredQueueNames = [.. registeredQueueNames];
        }

        // Update LastHeartbeatAt on all jobs registered by this node
        foreach (var jobName in registeredJobNames)
        {
            if (_jobs.TryGetValue(jobName, out var job))
                job.LastHeartbeatAt = _timeProvider.GetUtcNow();
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

    // -- Heartbeat & Inactive Runs --

    public Task HeartbeatRunsAsync(IReadOnlyCollection<string> runIds, CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        foreach (var runId in runIds)
        {
            if (_runs.TryGetValue(runId, out var run) && run.Status == JobStatus.Running)
                run.LastHeartbeatAt = now;
        }
        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<JobRun>> GetInactiveRunsAsync(TimeSpan threshold, CancellationToken cancellationToken = default)
    {
        var cutoff = _timeProvider.GetUtcNow() - threshold;
        var inactive = _runs.Values
            .Where(r => r.Status == JobStatus.Running && (r.LastHeartbeatAt is null || r.LastHeartbeatAt < cutoff))
            .ToList();
        return Task.FromResult<IReadOnlyList<JobRun>>(inactive);
    }

    // -- Trace --

    public Task<IReadOnlyList<JobRun>> GetRunTraceAsync(string runId, int limit = 200, CancellationToken cancellationToken = default)
    {
        // Walk up to find root
        var currentId = runId;
        while (currentId is not null && _runs.TryGetValue(currentId, out var current) && current.ParentRunId is not null)
        {
            if (!_runs.ContainsKey(current.ParentRunId))
                break; // Parent was purged
            currentId = current.ParentRunId;
        }

        // BFS down from root
        var result = new List<JobRun>();
        var queue = new Queue<string>();
        queue.Enqueue(currentId!);

        while (queue.Count > 0 && result.Count < limit)
        {
            var id = queue.Dequeue();
            if (!_runs.TryGetValue(id, out var run)) continue;
            result.Add(run);

            foreach (var child in _runs.Values.Where(r => r.ParentRunId == id))
                queue.Enqueue(child.Id);
        }

        result.Sort((a, b) => a.CreatedAt.CompareTo(b.CreatedAt));
        if (result.Count > limit)
            result.RemoveRange(limit, result.Count - limit);

        return Task.FromResult<IReadOnlyList<JobRun>>(result);
    }

    // -- Dashboard Stats --

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

        var now = _timeProvider.GetUtcNow();
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
                        case JobStatus.Skipped: bucket.Skipped++; break;
                    }
                }
            }
            buckets.Add(bucket);
        }

        return buckets;
    }

    // -- Job Stats --

    public Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken cancellationToken = default)
    {
        var jobRuns = _runs.Values.Where(r => r.JobName == jobName).ToList();

        var total = jobRuns.Count;
        var succeeded = jobRuns.Count(r => r.Status == JobStatus.Completed);
        var failed = jobRuns.Count(r => r.Status == JobStatus.Failed);
        var totalFinished = succeeded + failed;

        var durations = jobRuns
            .Where(r => r.Status is JobStatus.Completed or JobStatus.Failed && r.StartedAt is not null && r.CompletedAt is not null)
            .Select(r => (r.CompletedAt!.Value - r.StartedAt!.Value).TotalSeconds)
            .ToList();

        var avgDuration = durations.Count > 0 ? TimeSpan.FromSeconds(durations.Average()) : (TimeSpan?)null;
        var lastRunAt = jobRuns.Count > 0 ? jobRuns.Max(r => r.CreatedAt) : (DateTimeOffset?)null;

        return Task.FromResult(new JobStats
        {
            TotalRuns = total,
            SucceededRuns = succeeded,
            FailedRuns = failed,
            SuccessRate = totalFinished > 0 ? (double)succeeded / totalFinished * 100 : 0,
            AvgDuration = avgDuration,
            LastRunAt = lastRunAt
        });
    }

    // -- Queue Stats --

    public Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(CancellationToken cancellationToken = default)
    {
        var stats = new Dictionary<string, QueueStats>();

        foreach (var run in _runs.Values.Where(r => r.Status is JobStatus.Pending or JobStatus.Running))
        {
            var queueName = "default";
            if (_jobs.TryGetValue(run.JobName, out var jobDef))
                queueName = jobDef.Queue ?? "default";

            if (!stats.TryGetValue(queueName, out var qStats))
            {
                qStats = new QueueStats();
                stats[queueName] = qStats;
            }

            if (run.Status == JobStatus.Pending) qStats.PendingCount++;
            else if (run.Status == JobStatus.Running) qStats.RunningCount++;
        }

        return Task.FromResult<IReadOnlyDictionary<string, QueueStats>>(stats);
    }

    // -- Cancel Expired --

    public Task<int> CancelExpiredRunsAsync(CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        var cancelled = 0;

        foreach (var run in _runs.Values)
        {
            if (run.Status == JobStatus.Pending && run.NotAfter is not null && run.NotAfter < now)
            {
                run.Status = JobStatus.Cancelled;
                run.CancelledAt = now;
                cancelled++;
            }
        }

        return Task.FromResult(cancelled);
    }

    // -- Purge --

    public Task<int> PurgeRunsAsync(DateTimeOffset completedBefore, CancellationToken cancellationToken = default)
    {
        var purged = 0;
        // Snapshot keys to avoid iterating while mutating
        var keys = _runs.Keys.ToArray();
        foreach (var key in keys)
        {
            // Hold _claimLock to prevent racing with ClaimRunAsync which transitions
            // Pending -> Running. Without this lock, purge could read Status == Pending,
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

    public Task<int> PurgeJobsAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        var purged = 0;
        foreach (var kvp in _jobs)
        {
            if (kvp.Value.LastHeartbeatAt is not null && kvp.Value.LastHeartbeatAt < heartbeatBefore)
            {
                if (_jobs.TryRemove(kvp.Key, out _))
                    purged++;
            }
        }
        return Task.FromResult(purged);
    }

    public Task<int> PurgeQueuesAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        var purged = 0;
        foreach (var kvp in _queues)
        {
            if (kvp.Value.LastHeartbeatAt is not null && kvp.Value.LastHeartbeatAt < heartbeatBefore)
            {
                if (_queues.TryRemove(kvp.Key, out _))
                    purged++;
            }
        }
        return Task.FromResult(purged);
    }

    public Task<int> PurgeNodesAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default)
    {
        var purged = 0;
        foreach (var kvp in _nodes)
        {
            if (kvp.Value.LastHeartbeatAt < heartbeatBefore)
            {
                if (_nodes.TryRemove(kvp.Key, out _))
                    purged++;
            }
        }
        return Task.FromResult(purged);
    }

    public ValueTask DisposeAsync()
    {
        _rateLimiter.Dispose();
        return ValueTask.CompletedTask;
    }
}
