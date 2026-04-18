namespace Surefire;

internal sealed class InMemoryJobStore : IJobStore
{
    private readonly Dictionary<string, JobBatch> _batches = new();

    // Deduplication index scoped by (JobName, DeduplicationId) for non-terminal runs.
    private readonly Dictionary<string, List<RunEvent>> _batchEventsByBatchId = new();
    private readonly Dictionary<string, List<RunEvent>> _batchOutputEventsByBatchId = new();
    private readonly HashSet<(string JobName, string DeduplicationId)> _dedupIndex = [];
    private readonly Dictionary<string, List<RunEvent>> _eventsByRunId = new();
    private readonly Lock _gate = new();
    private readonly Dictionary<string, JobDefinition> _jobs = new();
    private readonly Dictionary<string, NodeInfo> _nodes = new();
    private readonly Dictionary<string, int> _nonTerminalCountByJob = new();
    private readonly Dictionary<string, int> _pendingCountByQueue = new();

    private readonly SortedSet<PendingRunKey> _pendingIndex = new();
    private readonly Dictionary<string, PendingRunKey> _pendingKeyByRunId = new();
    private readonly Dictionary<string, QueueDefinition> _queues = new();
    private readonly Dictionary<string, RateLimitDefinition> _rateLimitDefinitions = new();

    private readonly Dictionary<string, RateLimitWindowState> _rateLimitWindows = new();
    private readonly Dictionary<string, int> _runningCountByJob = new();
    private readonly Dictionary<string, int> _runningCountByQueue = new();
    private readonly Dictionary<string, JobRun> _runs = new();
    private readonly TimeProvider _timeProvider;
    private long _eventIdCounter;

    public InMemoryJobStore(TimeProvider timeProvider) => _timeProvider = timeProvider;

    public Task MigrateAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task PingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            if (_jobs.TryGetValue(job.Name, out var existing))
            {
                var updated = CopyJob(job);
                updated.IsEnabled = existing.IsEnabled;
                updated.LastHeartbeatAt = now;
                updated.LastCronFireAt = existing.LastCronFireAt;
                _jobs[job.Name] = updated;
            }
            else
            {
                var updated = CopyJob(job);
                updated.LastHeartbeatAt = now;
                updated.LastCronFireAt = null;
                _jobs[job.Name] = updated;
            }
        }

        return Task.CompletedTask;
    }

    public Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(_jobs.TryGetValue(name, out var job) ? CopyJob(job) : null);
        }
    }

    public Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            IEnumerable<JobDefinition> query = _jobs.Values;

            if (filter is { })
            {
                if (filter.Name is { })
                {
                    query = query.Where(j => j.Name.Contains(filter.Name, StringComparison.OrdinalIgnoreCase));
                }

                if (filter.Tag is { })
                {
                    query = query.Where(j => j.Tags.Contains(filter.Tag, StringComparer.OrdinalIgnoreCase));
                }

                if (filter.IsEnabled is { })
                {
                    query = query.Where(j => j.IsEnabled == filter.IsEnabled.Value);
                }

                if (filter.HeartbeatAfter is { })
                {
                    query = query.Where(j => j.LastHeartbeatAt > filter.HeartbeatAfter.Value);
                }
            }

            IReadOnlyList<JobDefinition> result = query.Select(CopyJob).OrderBy(j => j.Name).ToList();
            return Task.FromResult(result);
        }
    }

    public Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_jobs.TryGetValue(name, out var job))
            {
                job.IsEnabled = enabled;
            }
        }

        return Task.CompletedTask;
    }

    public Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_jobs.TryGetValue(jobName, out var job))
            {
                job.LastCronFireAt = fireAt;
            }
        }

        return Task.CompletedTask;
    }

    public Task CreateRunsAsync(IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => CreateRunsCoreAsync(runs, initialEvents);

    public Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default)
        => TryCreateRunCoreAsync(run, maxActiveForJob, lastCronFireAt, initialEvents);

    public Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(_runs.TryGetValue(id, out var run) ? run : null);
        }
    }

    public Task<IReadOnlyList<JobRun>> GetRunsByIdsAsync(IReadOnlyList<string> ids,
        CancellationToken cancellationToken = default)
    {
        if (ids.Count == 0)
        {
            return Task.FromResult<IReadOnlyList<JobRun>>([]);
        }

        lock (_gate)
        {
            var results = new List<JobRun>(ids.Count);
            foreach (var id in ids)
            {
                if (_runs.TryGetValue(id, out var run))
                {
                    results.Add(run);
                }
            }

            return Task.FromResult<IReadOnlyList<JobRun>>(results);
        }
    }

    public Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, int skip = 0, int take = 50,
        CancellationToken cancellationToken = default)
    {
        if (skip < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(skip));
        }

        if (take <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(take));
        }

        int totalCount;
        List<JobRun> items;

        lock (_gate)
        {
            IEnumerable<JobRun> query = _runs.Values;

            if (filter.Status is { })
            {
                query = query.Where(r => r.Status == filter.Status.Value);
            }

            if (filter.JobName is { })
            {
                if (filter.ExactJobName)
                {
                    query = query.Where(r => r.JobName == filter.JobName);
                }
                else
                {
                    query = query.Where(r => r.JobName.Contains(filter.JobName, StringComparison.OrdinalIgnoreCase));
                }
            }

            if (filter.ParentRunId is { })
            {
                query = query.Where(r => r.ParentRunId == filter.ParentRunId);
            }

            if (filter.RootRunId is { })
            {
                query = query.Where(r => r.RootRunId == filter.RootRunId);
            }

            if (filter.NodeName is { })
            {
                query = query.Where(r => r.NodeName == filter.NodeName);
            }

            if (filter.IsTerminal is { })
            {
                if (filter.IsTerminal.Value)
                {
                    query = query.Where(r => r.Status.IsTerminal);
                }
                else
                {
                    query = query.Where(r => !r.Status.IsTerminal);
                }
            }

            if (filter.BatchId is { })
            {
                query = query.Where(r => r.BatchId == filter.BatchId);
            }

            if (filter.CreatedAfter is { })
            {
                query = query.Where(r => r.CreatedAt > filter.CreatedAfter.Value);
            }

            if (filter.CreatedBefore is { })
            {
                query = query.Where(r => r.CreatedAt < filter.CreatedBefore.Value);
            }

            if (filter.CompletedAfter is { })
            {
                query = query.Where(r => r.CompletedAt > filter.CompletedAfter.Value);
            }

            if (filter.LastHeartbeatBefore is { })
            {
                query = query.Where(r => r.LastHeartbeatAt < filter.LastHeartbeatBefore.Value);
            }

            var all = query.ToList();
            totalCount = all.Count;

            IEnumerable<JobRun> ordered = filter.OrderBy switch
            {
                RunOrderBy.StartedAt => all
                    .OrderByDescending(r => r.StartedAt)
                    .ThenByDescending(r => r.Id),
                RunOrderBy.CompletedAt => all
                    .OrderByDescending(r => r.CompletedAt)
                    .ThenByDescending(r => r.Id),
                _ => all
                    .OrderByDescending(r => r.CreatedAt)
                    .ThenByDescending(r => r.Id)
            };

            items = ordered.Skip(skip).Take(take).ToList();
        }

        return Task.FromResult(new PagedResult<JobRun>
        {
            Items = items,
            TotalCount = totalCount
        });
    }

    public Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_runs.TryGetValue(run.Id, out var stored))
            {
                return Task.CompletedTask;
            }

            if (stored.Status.IsTerminal)
            {
                return Task.CompletedTask;
            }

            if (stored.NodeName != run.NodeName)
            {
                return Task.CompletedTask;
            }

            _runs[run.Id] = stored with
            {
                Progress = run.Progress,
                Result = run.Result,
                Reason = run.Reason,
                TraceId = run.TraceId,
                SpanId = run.SpanId,
                LastHeartbeatAt = run.LastHeartbeatAt
            };
        }

        return Task.CompletedTask;
    }

    public Task<RunTransitionResult> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_runs.TryGetValue(transition.RunId, out var stored))
            {
                return Task.FromResult(RunTransitionResult.NotApplied);
            }

            if (stored.Status != transition.ExpectedStatus || stored.Attempt != transition.ExpectedAttempt)
            {
                return Task.FromResult(RunTransitionResult.NotApplied);
            }

            if (stored.Status.IsTerminal)
            {
                return Task.FromResult(RunTransitionResult.NotApplied);
            }

            var oldStatus = stored.Status;
            var newStatus = transition.NewStatus;

            if (!RunTransitionRules.IsAllowed(transition.ExpectedStatus, newStatus) || !transition.HasRequiredFields())
            {
                return Task.FromResult(RunTransitionResult.NotApplied);
            }

            var newStored = stored with
            {
                Status = transition.NewStatus,
                NodeName = transition.NodeName,
                StartedAt = transition.StartedAt ?? stored.StartedAt,
                CompletedAt = transition.CompletedAt ?? stored.CompletedAt,
                CancelledAt = transition.CancelledAt ?? stored.CancelledAt,
                Reason = transition.Reason,
                Result = transition.Result,
                Progress = transition.Progress,
                NotBefore = transition.NotBefore,
                LastHeartbeatAt = transition.LastHeartbeatAt ?? stored.LastHeartbeatAt
            };
            _runs[newStored.Id] = newStored;

            UpdateIndexes(newStored, oldStatus, newStatus);
            AppendStatusEventCore(newStored.Id, newStored.Attempt, newStored.Status);
            AppendEventsCore(transition.Events);

            if (newStatus.IsTerminal && newStored.DeduplicationId is { })
            {
                _dedupIndex.Remove((newStored.JobName, newStored.DeduplicationId));
            }

            var batchCompletion = newStatus.IsTerminal
                ? IncrementBatchCounter(newStored.BatchId, newStatus, _timeProvider.GetUtcNow())
                : null;

            return Task.FromResult(new RunTransitionResult(true, batchCompletion));
        }
    }

    public Task<RunTransitionResult> TryCancelRunAsync(string runId,
        int? expectedAttempt = null,
        string? reason = null,
        IReadOnlyList<RunEvent>? events = null,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_runs.TryGetValue(runId, out var stored))
            {
                return Task.FromResult(RunTransitionResult.NotApplied);
            }

            if (stored.Status.IsTerminal)
            {
                return Task.FromResult(RunTransitionResult.NotApplied);
            }

            if (expectedAttempt is { } ea && stored.Attempt != ea)
            {
                return Task.FromResult(RunTransitionResult.NotApplied);
            }

            var oldStatus = stored.Status;
            var cancelledAt = _timeProvider.GetUtcNow();
            var cancelled = stored with
            {
                Status = JobStatus.Cancelled,
                CancelledAt = cancelledAt,
                CompletedAt = cancelledAt,
                Reason = reason
            };
            _runs[runId] = cancelled;

            UpdateIndexes(cancelled, oldStatus, JobStatus.Cancelled);
            AppendStatusEventCore(cancelled.Id, cancelled.Attempt, cancelled.Status);
            AppendEventsCore(events);

            if (cancelled.DeduplicationId is { })
            {
                _dedupIndex.Remove((cancelled.JobName, cancelled.DeduplicationId));
            }

            var batchCompletion = IncrementBatchCounter(cancelled.BatchId, JobStatus.Cancelled, cancelledAt);

            return Task.FromResult(new RunTransitionResult(true, batchCompletion));
        }
    }

    public Task<IReadOnlyList<string>> CancelChildRunsAsync(string parentRunId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        var cancelledIds = new List<string>();
        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            foreach (var run in _runs.Values.ToList())
            {
                if (run.ParentRunId != parentRunId || run.Status.IsTerminal)
                {
                    continue;
                }

                var oldStatus = run.Status;
                var cancelled = run with
                {
                    Status = JobStatus.Cancelled,
                    CancelledAt = now,
                    CompletedAt = now,
                    Reason = reason
                };
                _runs[run.Id] = cancelled;

                UpdateIndexes(cancelled, oldStatus, JobStatus.Cancelled);
                AppendStatusEventCore(cancelled.Id, cancelled.Attempt, cancelled.Status);

                if (cancelled.DeduplicationId is { })
                {
                    _dedupIndex.Remove((cancelled.JobName, cancelled.DeduplicationId));
                }

                IncrementBatchCounter(cancelled.BatchId, JobStatus.Cancelled, now);

                cancelledIds.Add(run.Id);
            }
        }

        return Task.FromResult<IReadOnlyList<string>>(cancelledIds);
    }

    public Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var now = _timeProvider.GetUtcNow();
            var jobNameSet = jobNames as ISet<string> ?? new HashSet<string>(jobNames);
            var queueNameSet = queueNames as ISet<string> ?? new HashSet<string>(queueNames);

            foreach (var key in _pendingIndex)
            {
                if (!_runs.TryGetValue(key.Id, out var run))
                {
                    continue;
                }

                if (run.Status != JobStatus.Pending)
                {
                    continue;
                }

                if (run.NotBefore > now)
                {
                    continue;
                }

                if (run.NotAfter is { } notAfter && notAfter <= now)
                {
                    continue;
                }

                if (!jobNameSet.Contains(run.JobName))
                {
                    continue;
                }

                if (!_jobs.TryGetValue(run.JobName, out var job))
                {
                    continue;
                }

                var queueName = job.Queue ?? "default";

                if (!queueNameSet.Contains(queueName))
                {
                    continue;
                }

                _queues.TryGetValue(queueName, out var queueDef);

                if (queueDef is { } && queueDef.IsPaused)
                {
                    continue;
                }

                if (job.MaxConcurrency is { } &&
                    GetCount(_runningCountByJob, run.JobName) >= job.MaxConcurrency.Value)
                {
                    continue;
                }

                if (queueDef?.MaxConcurrency is { } &&
                    GetCount(_runningCountByQueue, queueName) >= queueDef.MaxConcurrency.Value)
                {
                    continue;
                }

                // Rate limit: check both job and queue limits, then acquire both.
                // All under _lock so no race between check and acquire.
                var jobRateLimit = job.RateLimitName;
                var queueRateLimit = queueDef?.RateLimitName;

                if (!TryCheckRateLimit(jobRateLimit, now))
                {
                    continue;
                }

                if (queueRateLimit != jobRateLimit && !TryCheckRateLimit(queueRateLimit, now))
                {
                    continue;
                }

                AcquireRateLimit(jobRateLimit, now);
                if (queueRateLimit != jobRateLimit)
                {
                    AcquireRateLimit(queueRateLimit, now);
                }

                RemoveFromPendingIndex(run.Id);
                DecrementCount(_pendingCountByQueue, queueName);
                var claimed = run with
                {
                    Status = JobStatus.Running,
                    NodeName = nodeName,
                    StartedAt = now,
                    LastHeartbeatAt = now,
                    Attempt = run.Attempt + 1
                };
                _runs[run.Id] = claimed;

                IncrementCount(_runningCountByJob, claimed.JobName);
                IncrementCount(_runningCountByQueue, queueName);
                AppendStatusEventCore(claimed.Id, claimed.Attempt, claimed.Status);

                return Task.FromResult<JobRun?>(claimed);
            }

            return Task.FromResult<JobRun?>(null);
        }
    }

    public Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _batches[batch.Id] = batch;

            var copies = new List<JobRun>(runs.Count);
            foreach (var run in runs)
            {
                if (_runs.ContainsKey(run.Id))
                {
                    throw new InvalidOperationException($"Run '{run.Id}' already exists.");
                }

                copies.Add(run);
            }

            foreach (var run in copies)
            {
                var stored = run with { QueuePriority = GetQueuePriority(run.JobName) };
                _runs[stored.Id] = stored;

                if (stored.Status == JobStatus.Pending)
                {
                    AddToPendingIndex(stored);
                    IncrementCount(_pendingCountByQueue, GetQueueName(stored.JobName));
                }

                if (!stored.Status.IsTerminal)
                {
                    IncrementCount(_nonTerminalCountByJob, stored.JobName);
                }

                if (stored.DeduplicationId is { })
                {
                    _dedupIndex.Add((stored.JobName, stored.DeduplicationId));
                }
            }

            if (initialEvents?.Count > 0)
            {
                AppendEventsCore(initialEvents);
            }
        }

        return Task.CompletedTask;
    }

    public Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(_batches.TryGetValue(batchId, out var batch) ? batch : null);
        }
    }

    public Task<bool> TryCompleteBatchAsync(string batchId, JobStatus status, DateTimeOffset completedAt,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_batches.TryGetValue(batchId, out var batch) || batch.Status.IsTerminal)
            {
                return Task.FromResult(false);
            }

            _batches[batchId] = batch with { Status = status, CompletedAt = completedAt };
            return Task.FromResult(true);
        }
    }

    public Task<DirectChildrenPage> GetDirectChildrenAsync(string parentRunId,
        string? afterCursor = null,
        string? beforeCursor = null,
        int take = 50,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(take, 0);
        if (!string.IsNullOrEmpty(afterCursor) && !string.IsNullOrEmpty(beforeCursor))
        {
            throw new ArgumentException(
                "afterCursor and beforeCursor are mutually exclusive.", nameof(afterCursor));
        }

        var after = DirectChildrenPage.DecodeCursor(afterCursor);
        var before = DirectChildrenPage.DecodeCursor(beforeCursor);

        lock (_gate)
        {
            var query = _runs.Values.Where(r => r.ParentRunId == parentRunId);
            if (after is { } a)
            {
                query = query.Where(r => r.CreatedAt > a.CreatedAt
                                         || (r.CreatedAt == a.CreatedAt && string.CompareOrdinal(r.Id, a.Id) > 0));
            }

            if (before is { } b)
            {
                query = query.Where(r => r.CreatedAt < b.CreatedAt
                                         || (r.CreatedAt == b.CreatedAt && string.CompareOrdinal(r.Id, b.Id) < 0));
            }

            var ordered = before is null
                ? query.OrderBy(r => r.CreatedAt).ThenBy(r => r.Id, StringComparer.Ordinal)
                : query.OrderByDescending(r => r.CreatedAt).ThenByDescending(r => r.Id, StringComparer.Ordinal);

            // Keyset pagination with take+1 lookahead: NextCursor is non-null iff
            // a strictly additional row exists beyond the page boundary, per the
            // DirectChildrenPage contract.
            var items = ordered.Take(take + 1).ToList();
            var hasMore = items.Count > take;
            if (hasMore)
            {
                items.RemoveAt(items.Count - 1);
            }

            var nextCursor = hasMore
                ? DirectChildrenPage.EncodeCursor(items[^1].CreatedAt, items[^1].Id)
                : null;

            return Task.FromResult(new DirectChildrenPage { Items = items, NextCursor = nextCursor });
        }
    }

    public Task<IReadOnlyList<JobRun>> GetAncestorChainAsync(string runId,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var chain = new List<JobRun>();
            var visited = new HashSet<string>(StringComparer.Ordinal);
            if (!_runs.TryGetValue(runId, out var current))
            {
                return Task.FromResult<IReadOnlyList<JobRun>>(chain);
            }

            while (current.ParentRunId is { } parentId && visited.Add(parentId))
            {
                if (!_runs.TryGetValue(parentId, out var parent))
                {
                    break;
                }

                chain.Add(parent);
                current = parent;
            }

            chain.Reverse();
            return Task.FromResult<IReadOnlyList<JobRun>>(chain);
        }
    }

    public Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            AppendEventsCore(events);
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0, RunEventType[]? types = null,
        int? attempt = null, int? take = null, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_eventsByRunId.TryGetValue(runId, out var list))
            {
                return Task.FromResult<IReadOnlyList<RunEvent>>([]);
            }

            var query = list.Where(e => e.Id > sinceId);

            if (types is { })
            {
                var typeSet = new HashSet<RunEventType>(types);
                query = query.Where(e => typeSet.Contains(e.EventType));
            }

            if (attempt is { })
            {
                query = query.Where(e => e.Attempt == attempt.Value || e.Attempt == 0);
            }

            if (take is { })
            {
                query = query.Take(take.Value);
            }

            IReadOnlyList<RunEvent> result = query.Select(CopyEvent).ToList();
            return Task.FromResult(result);
        }
    }

    public Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchId, long sinceEventId = 0,
        int take = 200, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (take <= 0)
            {
                return Task.FromResult<IReadOnlyList<RunEvent>>([]);
            }

            if (!_batchOutputEventsByBatchId.TryGetValue(batchId, out var events))
            {
                return Task.FromResult<IReadOnlyList<RunEvent>>([]);
            }

            IReadOnlyList<RunEvent> result = events
                .Where(e => e.Id > sinceEventId)
                .Take(take)
                .Select(CopyEvent)
                .ToList();
            return Task.FromResult(result);
        }
    }

    public Task<IReadOnlyList<RunEvent>> GetBatchEventsAsync(string batchId, long sinceEventId = 0, int take = 200,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (take <= 0)
            {
                return Task.FromResult<IReadOnlyList<RunEvent>>([]);
            }

            if (!_batchEventsByBatchId.TryGetValue(batchId, out var events))
            {
                return Task.FromResult<IReadOnlyList<RunEvent>>([]);
            }

            IReadOnlyList<RunEvent> result = events
                .Where(e => e.Id > sinceEventId)
                .Take(take)
                .Select(CopyEvent)
                .ToList();
            return Task.FromResult(result);
        }
    }

    public Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, IReadOnlyCollection<string> activeRunIds,
        CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            if (_nodes.TryGetValue(nodeName, out var existing))
            {
                _nodes[nodeName] = existing with
                {
                    LastHeartbeatAt = now,
                    RunningCount = activeRunIds.Count,
                    RegisteredJobNames = jobNames.ToList(),
                    RegisteredQueueNames = queueNames.ToList()
                };
            }
            else
            {
                _nodes[nodeName] = new()
                {
                    Name = nodeName,
                    StartedAt = now,
                    LastHeartbeatAt = now,
                    RunningCount = activeRunIds.Count,
                    RegisteredJobNames = jobNames.ToList(),
                    RegisteredQueueNames = queueNames.ToList()
                };
            }

            foreach (var runId in activeRunIds)
            {
                if (_runs.TryGetValue(runId, out var run) && !run.Status.IsTerminal
                                                          && run.NodeName == nodeName)
                {
                    _runs[runId] = run with { LastHeartbeatAt = now };
                }
            }
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            IReadOnlyList<NodeInfo> result = _nodes.Values.Select(CopyNode).ToList();
            return Task.FromResult(result);
        }
    }

    public Task<NodeInfo?> GetNodeAsync(string name, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(_nodes.TryGetValue(name, out var node) ? CopyNode(node) : null);
        }
    }

    public Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            if (_queues.TryGetValue(queue.Name, out var existing))
            {
                var oldPriority = existing.Priority;
                var updated = CopyQueue(queue);
                updated.IsPaused = existing.IsPaused;
                updated.LastHeartbeatAt = now;
                _queues[queue.Name] = updated;

                if (oldPriority != updated.Priority)
                {
                    RebuildPendingIndexForQueue(queue.Name, updated.Priority);
                }
            }
            else
            {
                var updated = CopyQueue(queue);
                updated.LastHeartbeatAt = now;
                _queues[queue.Name] = updated;
            }
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            IReadOnlyList<QueueDefinition> result = _queues.Values.Select(CopyQueue).ToList();
            return Task.FromResult(result);
        }
    }

    public Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_queues.TryGetValue(name, out var queue))
            {
                queue.IsPaused = isPaused;
            }
            else
            {
                _queues[name] = new() { Name = name, IsPaused = isPaused };
            }
        }

        return Task.CompletedTask;
    }

    public Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            var copy = CopyRateLimit(rateLimit);
            copy.LastHeartbeatAt = now;
            _rateLimitDefinitions[rateLimit.Name] = copy;
        }

        return Task.CompletedTask;
    }

    public Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        var cancelledIds = new List<string>();

        lock (_gate)
        {
            foreach (var run in _runs.Values.ToList())
            {
                if (run.Status is not JobStatus.Pending)
                {
                    continue;
                }

                if (run.NotAfter is null || run.NotAfter.Value >= now)
                {
                    continue;
                }

                var oldStatus = run.Status;
                var cancelled = run with
                {
                    Status = JobStatus.Cancelled,
                    CancelledAt = now,
                    CompletedAt = now,
                    Reason = "Run expired past NotAfter deadline."
                };
                _runs[run.Id] = cancelled;

                if (cancelled.DeduplicationId is { })
                {
                    _dedupIndex.Remove((cancelled.JobName, cancelled.DeduplicationId));
                }

                UpdateIndexes(cancelled, oldStatus, JobStatus.Cancelled);
                AppendStatusEventCore(cancelled.Id, cancelled.Attempt, cancelled.Status);
                IncrementBatchCounter(cancelled.BatchId, JobStatus.Cancelled, now);
                cancelledIds.Add(run.Id);
            }
        }

        return Task.FromResult<IReadOnlyList<string>>(cancelledIds);
    }

    public Task PurgeAsync(DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var runsToRemove = _runs.Values
                .Where(r =>
                    (CanPurgeTerminalRun(r, threshold) && r.CompletedAt < threshold) ||
                    (r.Status == JobStatus.Pending && r.NotBefore < threshold))
                .Select(r => r.Id)
                .ToList();

            foreach (var id in runsToRemove)
            {
                if (_runs.Remove(id, out var removed))
                {
                    RemoveFromPendingIndex(id);
                    if (_eventsByRunId.Remove(id, out var removedEvents))
                    {
                        RemoveBatchEventIndexes(removed, removedEvents);
                    }

                    var queueName = GetQueueName(removed.JobName);

                    if (removed.Status == JobStatus.Pending)
                    {
                        DecrementCount(_pendingCountByQueue, queueName);
                    }

                    if (ConsumesWorkerCapacity(removed.Status))
                    {
                        DecrementCount(_runningCountByJob, removed.JobName);
                        DecrementCount(_runningCountByQueue, queueName);
                    }

                    if (removed.DeduplicationId is { })
                    {
                        _dedupIndex.Remove((removed.JobName, removed.DeduplicationId));
                    }

                    if (!removed.Status.IsTerminal)
                    {
                        DecrementCount(_nonTerminalCountByJob, removed.JobName);
                    }
                }
            }

            var jobsWithActiveRuns = new HashSet<string>(
                _runs.Values.Where(r => !r.Status.IsTerminal).Select(r => r.JobName));

            var jobsToRemove = _jobs.Values
                .Where(j => j.LastHeartbeatAt < threshold && !jobsWithActiveRuns.Contains(j.Name))
                .Select(j => j.Name)
                .ToList();

            foreach (var name in jobsToRemove)
            {
                _jobs.Remove(name);
            }

            var queuesToRemove = _queues.Values
                .Where(q => q.LastHeartbeatAt < threshold)
                .Select(q => q.Name)
                .ToList();

            foreach (var name in queuesToRemove)
            {
                _queues.Remove(name);
            }

            var rateLimitsToRemove = _rateLimitDefinitions.Values
                .Where(r => r.LastHeartbeatAt < threshold)
                .Select(r => r.Name)
                .ToList();

            foreach (var name in rateLimitsToRemove)
            {
                _rateLimitDefinitions.Remove(name);
                _rateLimitWindows.Remove(name);
            }

            var nodesToRemove = _nodes.Values
                .Where(n => n.LastHeartbeatAt < threshold)
                .Select(n => n.Name)
                .ToList();

            foreach (var name in nodesToRemove)
            {
                _nodes.Remove(name);
            }

            var batchesToRemove = _batches.Values
                .Where(b => b.Status.IsTerminal && b.CompletedAt < threshold)
                .Select(b => b.Id)
                .ToList();

            foreach (var id in batchesToRemove)
            {
                _batches.Remove(id);
                _batchEventsByBatchId.Remove(id);
                _batchOutputEventsByBatchId.Remove(id);
            }
        }

        return Task.CompletedTask;
    }

    public Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60,
        CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        var sinceTime = since ?? now.AddHours(-24);
        if (bucketMinutes <= 0)
        {
            bucketMinutes = 60;
        }

        var bucketSpan = TimeSpan.FromMinutes(bucketMinutes);

        // Pre-compute bucket boundaries
        var bucketCount = 0;
        var temp = sinceTime;
        while (temp < now)
        {
            bucketCount++;
            temp += bucketSpan;
        }

        var bucketPending = new int[bucketCount];
        var bucketRunning = new int[bucketCount];
        var bucketSucceeded = new int[bucketCount];
        var bucketCancelled = new int[bucketCount];
        var bucketFailed = new int[bucketCount];

        var statusCounts = new Dictionary<string, int>();
        int totalRuns;
        var completedCount = 0;
        var terminalCount = 0;
        var activeRuns = 0;
        int jobCount;
        int nodeCount;

        lock (_gate)
        {
            totalRuns = _runs.Count;
            jobCount = _jobs.Count;
            var nodeThreshold = now - TimeSpan.FromMinutes(2);
            nodeCount = _nodes.Values.Count(n => n.LastHeartbeatAt >= nodeThreshold);

            foreach (var run in _runs.Values)
            {
                var statusName = run.Status.ToString();
                statusCounts[statusName] = statusCounts.TryGetValue(statusName, out var sc) ? sc + 1 : 1;

                if (run.Status.IsTerminal)
                {
                    terminalCount++;
                    if (run.Status == JobStatus.Succeeded)
                    {
                        completedCount++;
                    }
                }
                else
                {
                    activeRuns++;
                }

                var bucketTimestamp = GetTimelineBucketTimestamp(run);
                if (bucketTimestamp >= sinceTime && bucketTimestamp < now)
                {
                    var bucketIndex = (int)((bucketTimestamp - sinceTime).Ticks / bucketSpan.Ticks);
                    if (bucketIndex >= 0 && bucketIndex < bucketCount)
                    {
                        switch (run.Status)
                        {
                            case JobStatus.Pending:
                                bucketPending[bucketIndex]++;
                                break;
                            case JobStatus.Running:
                                bucketRunning[bucketIndex]++;
                                break;
                            case JobStatus.Succeeded:
                                bucketSucceeded[bucketIndex]++;
                                break;
                            case JobStatus.Cancelled:
                                bucketCancelled[bucketIndex]++;
                                break;
                            case JobStatus.Failed:
                                bucketFailed[bucketIndex]++;
                                break;
                        }
                    }
                }
            }
        }

        var successRate = terminalCount > 0 ? completedCount / (double)terminalCount : 0.0;

        var buckets = new List<TimelineBucket>(bucketCount);
        var bucketStart = sinceTime;
        for (var i = 0; i < bucketCount; i++)
        {
            buckets.Add(new()
            {
                Start = bucketStart,
                Pending = bucketPending[i],
                Running = bucketRunning[i],
                Succeeded = bucketSucceeded[i],
                Cancelled = bucketCancelled[i],
                Failed = bucketFailed[i]
            });
            bucketStart += bucketSpan;
        }

        var stats = new DashboardStats
        {
            TotalJobs = jobCount,
            TotalRuns = totalRuns,
            ActiveRuns = activeRuns,
            SuccessRate = successRate,
            NodeCount = nodeCount,
            RunsByStatus = statusCounts,
            Timeline = buckets
        };

        return Task.FromResult(stats);
    }

    public Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken cancellationToken = default)
    {
        var totalRuns = 0;
        var completedCount = 0;
        var failedCount = 0;
        var terminalCount = 0;
        long durationTicksSum = 0;
        var durationCount = 0;
        DateTimeOffset? lastRunAt = null;

        lock (_gate)
        {
            foreach (var run in _runs.Values)
            {
                if (run.JobName != jobName)
                {
                    continue;
                }

                totalRuns++;

                if (run.StartedAt is { } startedAt)
                {
                    if (lastRunAt is null || startedAt > lastRunAt.Value)
                    {
                        lastRunAt = startedAt;
                    }
                }

                if (run.Status.IsTerminal)
                {
                    terminalCount++;
                    if (run.Status == JobStatus.Succeeded)
                    {
                        completedCount++;
                        if (run.StartedAt is { } s && run.CompletedAt is { } c)
                        {
                            durationTicksSum += (c - s).Ticks;
                            durationCount++;
                        }
                    }
                    else if (run.Status == JobStatus.Failed)
                    {
                        failedCount++;
                    }
                }
            }
        }

        var successRate = terminalCount > 0 ? completedCount / (double)terminalCount : 0.0;
        var avgDuration = durationCount > 0
            ? TimeSpan.FromTicks(durationTicksSum / durationCount)
            : (TimeSpan?)null;

        var stats = new JobStats
        {
            TotalRuns = totalRuns,
            SucceededRuns = completedCount,
            FailedRuns = failedCount,
            SuccessRate = successRate,
            AvgDuration = avgDuration,
            LastRunAt = lastRunAt
        };

        return Task.FromResult(stats);
    }

    public Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(
        CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, QueueStats>();

        lock (_gate)
        {
            var queueNames = new HashSet<string>(_queues.Keys);
            foreach (var name in _pendingCountByQueue.Keys)
            {
                queueNames.Add(name);
            }

            foreach (var name in _runningCountByQueue.Keys)
            {
                queueNames.Add(name);
            }

            foreach (var queueName in queueNames)
            {
                var pendingCount = GetCount(_pendingCountByQueue, queueName);
                var runningCount = GetCount(_runningCountByQueue, queueName);

                result[queueName] = new()
                {
                    PendingCount = pendingCount,
                    RunningCount = runningCount
                };
            }
        }

        return Task.FromResult<IReadOnlyDictionary<string, QueueStats>>(result);
    }

    public Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var result = new List<string>();
            foreach (var (batchId, batch) in _batches)
            {
                if (batch.Status.IsTerminal)
                {
                    continue;
                }

                var allTerminal = !_runs.Values.Any(r => r.BatchId == batchId && !r.Status.IsTerminal);
                if (allTerminal)
                {
                    result.Add(batchId);
                }
            }

            return Task.FromResult<IReadOnlyList<string>>(result);
        }
    }

    public Task<IReadOnlyList<string>> CancelBatchRunsAsync(string batchId,
        string? reason = null,
        CancellationToken cancellationToken = default)
    {
        var cancelledIds = new List<string>();
        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            foreach (var run in _runs.Values.ToList())
            {
                if (run.BatchId != batchId || run.Status.IsTerminal)
                {
                    continue;
                }

                var oldStatus = run.Status;
                var cancelled = run with
                {
                    Status = JobStatus.Cancelled,
                    CancelledAt = now,
                    CompletedAt = now,
                    Reason = reason
                };
                _runs[run.Id] = cancelled;

                UpdateIndexes(cancelled, oldStatus, JobStatus.Cancelled);
                AppendStatusEventCore(cancelled.Id, cancelled.Attempt, cancelled.Status);

                if (cancelled.DeduplicationId is { })
                {
                    _dedupIndex.Remove((cancelled.JobName, cancelled.DeduplicationId));
                }

                IncrementBatchCounter(batchId, JobStatus.Cancelled, now);

                cancelledIds.Add(run.Id);
            }
        }

        return Task.FromResult<IReadOnlyList<string>>(cancelledIds);
    }

    public Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(
        IReadOnlyCollection<string> runIds, CancellationToken cancellationToken = default)
    {
        if (runIds.Count == 0)
        {
            return Task.FromResult<IReadOnlyList<string>>([]);
        }

        lock (_gate)
        {
            var stopped = new List<string>();
            foreach (var runId in runIds)
            {
                if (!_runs.TryGetValue(runId, out var run) || run.Status != JobStatus.Running)
                {
                    stopped.Add(runId);
                }
            }

            return Task.FromResult<IReadOnlyList<string>>(stopped);
        }
    }

    private static DateTimeOffset GetTimelineBucketTimestamp(JobRun run) => run.Status switch
    {
        JobStatus.Running => run.StartedAt ?? run.CreatedAt,
        JobStatus.Succeeded => run.CompletedAt ?? run.StartedAt ?? run.CreatedAt,
        JobStatus.Cancelled => run.CancelledAt ?? run.CompletedAt ?? run.CreatedAt,
        JobStatus.Failed => run.CompletedAt ?? run.StartedAt ?? run.CreatedAt,
        _ => run.CreatedAt
    };

    private Task CreateRunsCoreAsync(IReadOnlyList<JobRun> runs, IReadOnlyList<RunEvent>? initialEvents)
    {
        lock (_gate)
        {
            var copies = new List<JobRun>(runs.Count);
            var seenIds = new HashSet<string>(runs.Count);
            foreach (var run in runs)
            {
                if (_runs.ContainsKey(run.Id) || !seenIds.Add(run.Id))
                {
                    throw new InvalidOperationException($"Run '{run.Id}' already exists.");
                }

                copies.Add(run);
            }

            foreach (var run in copies)
            {
                var stored = run with { QueuePriority = GetQueuePriority(run.JobName) };
                _runs[stored.Id] = stored;

                if (stored.Status == JobStatus.Pending)
                {
                    AddToPendingIndex(stored);
                    IncrementCount(_pendingCountByQueue, GetQueueName(stored.JobName));
                }
                else if (ConsumesWorkerCapacity(stored.Status))
                {
                    IncrementCount(_runningCountByJob, stored.JobName);
                    IncrementCount(_runningCountByQueue, GetQueueName(stored.JobName));
                }

                if (stored.DeduplicationId is { })
                {
                    _dedupIndex.Add((stored.JobName, stored.DeduplicationId));
                }

                if (!stored.Status.IsTerminal)
                {
                    IncrementCount(_nonTerminalCountByJob, stored.JobName);
                }
            }

            AppendEventsCore(initialEvents);
        }

        return Task.CompletedTask;
    }

    private Task<bool> TryCreateRunCoreAsync(JobRun run, int? maxActiveForJob,
        DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents)
    {
        lock (_gate)
        {
            if (run.DeduplicationId is { })
            {
                if (_dedupIndex.Contains((run.JobName, run.DeduplicationId)))
                {
                    return Task.FromResult(false);
                }
            }

            if (maxActiveForJob is { })
            {
                if (_jobs.TryGetValue(run.JobName, out var job) && !job.IsEnabled)
                {
                    return Task.FromResult(false);
                }

                _nonTerminalCountByJob.TryGetValue(run.JobName, out var activeCount);

                if (activeCount >= maxActiveForJob.Value)
                {
                    return Task.FromResult(false);
                }
            }

            var stored = run with { QueuePriority = GetQueuePriority(run.JobName) };
            if (_runs.ContainsKey(stored.Id))
            {
                return Task.FromResult(false);
            }

            _runs[stored.Id] = stored;

            if (stored.Status == JobStatus.Pending)
            {
                AddToPendingIndex(stored);
                IncrementCount(_pendingCountByQueue, GetQueueName(stored.JobName));
            }
            else if (ConsumesWorkerCapacity(stored.Status))
            {
                IncrementCount(_runningCountByJob, stored.JobName);
                IncrementCount(_runningCountByQueue, GetQueueName(stored.JobName));
            }

            if (stored.DeduplicationId is { })
            {
                _dedupIndex.Add((stored.JobName, stored.DeduplicationId));
            }

            if (!stored.Status.IsTerminal)
            {
                IncrementCount(_nonTerminalCountByJob, stored.JobName);
            }

            if (lastCronFireAt is { } fireAt && _jobs.TryGetValue(run.JobName, out var jobDef))
            {
                jobDef.LastCronFireAt = fireAt;
            }

            AppendEventsCore(initialEvents);

            return Task.FromResult(true);
        }
    }

    private void AppendEventsCore(IReadOnlyList<RunEvent>? events)
    {
        if (events is null || events.Count == 0)
        {
            return;
        }

        foreach (var evt in events)
        {
            var copy = evt with { Id = ++_eventIdCounter };

            if (!_eventsByRunId.TryGetValue(copy.RunId, out var list))
            {
                list = new();
                _eventsByRunId[copy.RunId] = list;
            }

            list.Add(copy);

            if (_runs.TryGetValue(copy.RunId, out var run) && run.BatchId is { } batchId)
            {
                AppendBatchEvent(_batchEventsByBatchId, batchId, copy);

                if (copy.EventType == RunEventType.Output)
                {
                    AppendBatchEvent(_batchOutputEventsByBatchId, batchId, copy);
                }
            }
        }
    }

    private bool CanPurgeTerminalRun(JobRun run, DateTimeOffset threshold)
    {
        if (!run.Status.IsTerminal || run.CompletedAt is null)
        {
            return false;
        }

        if (run.BatchId is null)
        {
            return true;
        }

        if (!_batches.TryGetValue(run.BatchId, out var batch))
        {
            return true;
        }

        return batch.Status.IsTerminal
               && batch.CompletedAt is { } batchCompletedAt
               && batchCompletedAt < threshold;
    }

    private void AddToPendingIndex(JobRun run)
    {
        var key = new PendingRunKey(run.QueuePriority, run.Priority, run.NotBefore, run.Id);
        _pendingIndex.Add(key);
        _pendingKeyByRunId[run.Id] = key;
    }

    private void RemoveFromPendingIndex(string runId)
    {
        if (_pendingKeyByRunId.Remove(runId, out var key))
        {
            _pendingIndex.Remove(key);
        }
    }

    private void RebuildPendingIndexForQueue(string queueName, int newPriority)
    {
        var toUpdate = new List<(string RunId, PendingRunKey OldKey)>();
        foreach (var (runId, oldKey) in _pendingKeyByRunId)
        {
            if (_runs.TryGetValue(runId, out var run) && GetQueueName(run.JobName) == queueName)
            {
                toUpdate.Add((runId, oldKey));
            }
        }

        foreach (var (runId, oldKey) in toUpdate)
        {
            _pendingIndex.Remove(oldKey);
            var run = _runs[runId];
            var updated = run with { QueuePriority = newPriority };
            _runs[runId] = updated;
            var newKey = new PendingRunKey(newPriority, run.Priority, run.NotBefore, run.Id);
            _pendingIndex.Add(newKey);
            _pendingKeyByRunId[runId] = newKey;
        }
    }

    private void UpdateIndexes(JobRun run, JobStatus oldStatus, JobStatus newStatus)
    {
        if (oldStatus == newStatus)
        {
            return;
        }

        var queueName = GetQueueName(run.JobName);

        if (oldStatus == JobStatus.Pending)
        {
            RemoveFromPendingIndex(run.Id);
            DecrementCount(_pendingCountByQueue, queueName);
        }

        if (ConsumesWorkerCapacity(oldStatus))
        {
            DecrementCount(_runningCountByJob, run.JobName);
            DecrementCount(_runningCountByQueue, queueName);
        }

        if (newStatus == JobStatus.Pending)
        {
            AddToPendingIndex(run);
            IncrementCount(_pendingCountByQueue, queueName);
        }

        if (ConsumesWorkerCapacity(newStatus))
        {
            IncrementCount(_runningCountByJob, run.JobName);
            IncrementCount(_runningCountByQueue, queueName);
        }

        if (!oldStatus.IsTerminal && newStatus.IsTerminal)
        {
            DecrementCount(_nonTerminalCountByJob, run.JobName);
        }
    }

    private string GetQueueName(string jobName) =>
        _jobs.TryGetValue(jobName, out var job) ? job.Queue ?? "default" : "default";

    private static bool ConsumesWorkerCapacity(JobStatus status) =>
        status is JobStatus.Running;

    private void AppendStatusEventCore(string runId, int attempt, JobStatus status) =>
        AppendEventsCore([RunStatusEvents.Create(runId, attempt, status, _timeProvider.GetUtcNow())]);

    private int GetQueuePriority(string jobName)
    {
        if (_jobs.TryGetValue(jobName, out var job))
        {
            var queueName = job.Queue ?? "default";
            if (_queues.TryGetValue(queueName, out var queue))
            {
                return queue.Priority;
            }
        }

        return 0;
    }

    private static void IncrementCount(Dictionary<string, int> dict, string key)
    {
        dict[key] = dict.TryGetValue(key, out var count) ? count + 1 : 1;
    }

    private static void DecrementCount(Dictionary<string, int> dict, string key)
    {
        if (dict.TryGetValue(key, out var count))
        {
            if (count <= 1)
            {
                dict.Remove(key);
            }
            else
            {
                dict[key] = count - 1;
            }
        }
    }

    private static int GetCount(Dictionary<string, int> dict, string key) =>
        dict.TryGetValue(key, out var count) ? count : 0;

    // Same algorithm used by all stores (SQL inline, Redis Lua, InMemory).
    // FixedWindow: count resets when window elapses.
    // SlidingWindow: effective_count = current + previous * (1 - elapsed/window).

    private bool TryCheckRateLimit(string? rateLimitName, DateTimeOffset now)
    {
        if (rateLimitName is null)
        {
            return true;
        }

        if (!_rateLimitDefinitions.TryGetValue(rateLimitName, out var def))
        {
            return true;
        }

        var window = GetOrCreateWindow(rateLimitName);
        RotateWindowIfExpired(window, def, now);

        var effectiveCount = def.Type == RateLimitType.SlidingWindow
            ? GetSlidingWindowCount(window, def, now)
            : window.CurrentCount;

        return effectiveCount < def.MaxPermits;
    }

    private void AcquireRateLimit(string? rateLimitName, DateTimeOffset now)
    {
        if (rateLimitName is null)
        {
            return;
        }

        if (!_rateLimitDefinitions.TryGetValue(rateLimitName, out var def))
        {
            return;
        }

        var window = GetOrCreateWindow(rateLimitName);
        RotateWindowIfExpired(window, def, now);
        window.CurrentCount++;
    }

    private RateLimitWindowState GetOrCreateWindow(string name)
    {
        if (!_rateLimitWindows.TryGetValue(name, out var window))
        {
            window = new();
            _rateLimitWindows[name] = window;
        }

        return window;
    }

    private static void RotateWindowIfExpired(RateLimitWindowState window, RateLimitDefinition def, DateTimeOffset now)
    {
        if (window.WindowStart == default)
        {
            window.WindowStart = now;
            return;
        }

        var elapsed = now - window.WindowStart;
        if (elapsed < def.Window)
        {
            return;
        }

        var windowsElapsed = (long)(elapsed / def.Window);
        if (windowsElapsed >= 2)
        {
            window.PreviousCount = 0;
            window.CurrentCount = 0;
            window.WindowStart += def.Window * windowsElapsed;
        }
        else
        {
            window.PreviousCount = window.CurrentCount;
            window.CurrentCount = 0;
            window.WindowStart += def.Window;
        }
    }

    private static double GetSlidingWindowCount(RateLimitWindowState window, RateLimitDefinition def,
        DateTimeOffset now)
    {
        var elapsed = now - window.WindowStart;
        var weight = Math.Max(0, 1.0 - elapsed.TotalMilliseconds / def.Window.TotalMilliseconds);
        return window.CurrentCount + window.PreviousCount * weight;
    }

    private static RunEvent CopyEvent(RunEvent e) => new()
    {
        Id = e.Id,
        RunId = e.RunId,
        EventType = e.EventType,
        Payload = e.Payload,
        CreatedAt = e.CreatedAt,
        Attempt = e.Attempt
    };

    private static JobDefinition CopyJob(JobDefinition job) => new()
    {
        Name = job.Name,
        Description = job.Description,
        Tags = [.. job.Tags],
        CronExpression = job.CronExpression,
        TimeZoneId = job.TimeZoneId,
        Timeout = job.Timeout,
        MaxConcurrency = job.MaxConcurrency,
        Priority = job.Priority,
        RetryPolicy = job.RetryPolicy with { },
        IsContinuous = job.IsContinuous,
        Queue = job.Queue,
        RateLimitName = job.RateLimitName,
        IsEnabled = job.IsEnabled,
        MisfirePolicy = job.MisfirePolicy,
        FireAllLimit = job.FireAllLimit,
        ArgumentsSchema = job.ArgumentsSchema,
        LastHeartbeatAt = job.LastHeartbeatAt,
        LastCronFireAt = job.LastCronFireAt
    };

    private static QueueDefinition CopyQueue(QueueDefinition queue) => new()
    {
        Name = queue.Name,
        Priority = queue.Priority,
        MaxConcurrency = queue.MaxConcurrency,
        IsPaused = queue.IsPaused,
        RateLimitName = queue.RateLimitName,
        LastHeartbeatAt = queue.LastHeartbeatAt
    };

    private static NodeInfo CopyNode(NodeInfo node) => node with
    {
        RegisteredJobNames = node.RegisteredJobNames.ToList(),
        RegisteredQueueNames = node.RegisteredQueueNames.ToList()
    };

    private static RateLimitDefinition CopyRateLimit(RateLimitDefinition def) => new()
    {
        Name = def.Name,
        Type = def.Type,
        MaxPermits = def.MaxPermits,
        Window = def.Window,
        LastHeartbeatAt = def.LastHeartbeatAt
    };

    private BatchCompletionInfo? IncrementBatchCounter(string? batchId, JobStatus terminalStatus,
        DateTimeOffset completedAt)
    {
        if (batchId is null || !_batches.TryGetValue(batchId, out var batch) || batch.Status.IsTerminal)
        {
            return null;
        }

        batch = terminalStatus switch
        {
            JobStatus.Succeeded => batch with { Succeeded = batch.Succeeded + 1 },
            JobStatus.Failed => batch with { Failed = batch.Failed + 1 },
            JobStatus.Cancelled => batch with { Cancelled = batch.Cancelled + 1 },
            _ => batch
        };

        BatchCompletionInfo? batchCompletion = null;
        if (batch.Succeeded + batch.Failed + batch.Cancelled >= batch.Total)
        {
            var batchStatus = batch.Failed > 0 ? JobStatus.Failed
                : batch.Cancelled > 0 ? JobStatus.Cancelled
                : JobStatus.Succeeded;
            batch = batch with { Status = batchStatus, CompletedAt = completedAt };
            batchCompletion = new(batchId, batchStatus, completedAt);
        }

        _batches[batchId] = batch;
        return batchCompletion;
    }

    private static void AppendBatchEvent(Dictionary<string, List<RunEvent>> index, string batchId, RunEvent @event)
    {
        if (!index.TryGetValue(batchId, out var list))
        {
            list = [];
            index[batchId] = list;
        }

        list.Add(@event);
    }

    private void RemoveBatchEventIndexes(JobRun run, List<RunEvent> removedEvents)
    {
        if (run.BatchId is not { } batchId || removedEvents.Count == 0)
        {
            return;
        }

        RemoveBatchEventIndexEntries(_batchEventsByBatchId, batchId, removedEvents);
        RemoveBatchEventIndexEntries(_batchOutputEventsByBatchId, batchId,
            removedEvents.Where(e => e.EventType == RunEventType.Output).ToList());
    }

    private static void RemoveBatchEventIndexEntries(Dictionary<string, List<RunEvent>> index, string batchId,
        List<RunEvent> removedEvents)
    {
        if (removedEvents.Count == 0 || !index.TryGetValue(batchId, out var list))
        {
            return;
        }

        var removedIds = removedEvents.Select(e => e.Id).ToHashSet();
        list.RemoveAll(e => removedIds.Contains(e.Id));

        if (list.Count == 0)
        {
            index.Remove(batchId);
        }
    }

    private sealed class RateLimitWindowState
    {
        public int CurrentCount;
        public int PreviousCount;
        public DateTimeOffset WindowStart;
    }

    // Sort order: queue priority DESC, run priority DESC, NotBefore ASC, Id ASC
    private readonly record struct PendingRunKey(
        int QueuePriority,
        int RunPriority,
        DateTimeOffset NotBefore,
        string Id) : IComparable<PendingRunKey>
    {
        public int CompareTo(PendingRunKey other)
        {
            var cmp = other.QueuePriority.CompareTo(QueuePriority);
            if (cmp != 0)
            {
                return cmp;
            }

            cmp = other.RunPriority.CompareTo(RunPriority);
            if (cmp != 0)
            {
                return cmp;
            }

            cmp = NotBefore.CompareTo(other.NotBefore);
            if (cmp != 0)
            {
                return cmp;
            }

            return string.Compare(Id, other.Id, StringComparison.Ordinal);
        }
    }
}