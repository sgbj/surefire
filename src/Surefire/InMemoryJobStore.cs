namespace Surefire;

/// <summary>
///     Single-process in-memory <see cref="IJobStore" /> used by tests and dev fixtures.
///     <para>
///         Intended for dev/test, not production: claim iterates every pending run and sorts them
///         under <c>_gate</c>, which is O(n log n) per claim. The SQL / Redis stores index
///         pending runs server-side and have O(log n) per-claim cost. Avoid this store for
///         benchmarks or deployments with large pending backlogs.
///     </para>
/// </summary>
internal sealed class InMemoryJobStore : IJobStore
{
    private readonly Dictionary<string, JobBatch> _batches = new();

    private readonly Dictionary<string, List<string>> _batchesByParentRun =
        new(StringComparer.Ordinal);

    private readonly Dictionary<string, List<RunEvent>> _batchEventsByBatchId = new();
    private readonly Dictionary<string, List<RunEvent>> _batchOutputEventsByBatchId = new();

    private readonly Dictionary<string, List<string>> _childrenByParent =
        new(StringComparer.Ordinal);

    // (JobName, DeduplicationId) of non-terminal runs.
    private readonly HashSet<(string JobName, string DeduplicationId)> _dedupIndex = [];

    private readonly Dictionary<(string OrchestratorRunId, int Step), DurableRecord> _durableRecords = new();

    private readonly Dictionary<string, HashSet<string>> _durableWaitsByOrchestratorBatch =
        new(StringComparer.Ordinal);

    // Wait table modeled as in-process: outgoing (orchestrator -> awaited entities) and incoming
    // (awaited entity -> orchestrators waiting). Both directions kept in sync so wake-on-terminal
    // looks up reverse, suspend-replay inserts both, and run-cancel clears outgoing without
    // scanning. HashSet for uniqueness (re-suspend reuses ids) and O(1) remove.
    private readonly Dictionary<string, HashSet<string>> _durableWaitsByOrchestratorRun =
        new(StringComparer.Ordinal);

    private readonly Dictionary<string, List<RunEvent>> _eventsByRunId = new();
    private readonly Lock _gate = new();
    private readonly Dictionary<string, JobDefinition> _jobs = new();
    private readonly Dictionary<string, NodeInfo> _nodes = new();
    private readonly Dictionary<string, int> _nonTerminalCountByJob = new();

    private readonly Dictionary<string, HashSet<string>> _orchestratorsAwaitingBatch =
        new(StringComparer.Ordinal);

    private readonly Dictionary<string, HashSet<string>> _orchestratorsAwaitingRun =
        new(StringComparer.Ordinal);

    // Per-run pending entries keyed by runId. Sorted globally at claim time using
    // the live queue priority from _queues, so queue priority changes take effect on
    // the very next claim without any index rebuild.
    private readonly Dictionary<string, PendingRunEntry> _pending = new();
    private readonly Dictionary<string, int> _pendingCountByQueue = new();
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

    public bool IsTransientException(Exception ex) => false;

    public Task UpsertJobsAsync(IReadOnlyList<JobDefinition> jobs, CancellationToken cancellationToken = default)
    {
        if (jobs.Count == 0)
        {
            return Task.CompletedTask;
        }

        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            foreach (var job in jobs)
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
        => CreateRunsAsyncCore(runs, initialEvents);

    public Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        DurableStepRecord? durableStepRecord = null,
        CancellationToken cancellationToken = default)
        => TryCreateRunAsyncCore(run, maxActiveForJob, lastCronFireAt, initialEvents, durableStepRecord);

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

            if (filter.JobName is { } exactJobName)
            {
                query = query.Where(r => r.JobName == exactJobName);
            }

            if (filter.JobNameContains is { } jobNameContains)
            {
                query = query.Where(r => r.JobName.Contains(jobNameContains, StringComparison.OrdinalIgnoreCase));
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

            var ascending = filter.Direction == RunOrderDirection.Ascending;
            // Contract: nulls always sort last. Saturate null timestamps to MaxValue for ASC
            // and MinValue for DESC so they end up at the "tail" of either direction. Tie-break
            // by Id with ordinal comparison to match the SQL stores' binary collation.
            IEnumerable<JobRun> ordered = (filter.OrderBy, ascending) switch
            {
                (RunOrderBy.StartedAt, false) => all
                    .OrderByDescending(r => r.StartedAt ?? DateTimeOffset.MinValue)
                    .ThenByDescending(r => r.Id, StringComparer.Ordinal),
                (RunOrderBy.StartedAt, true) => all
                    .OrderBy(r => r.StartedAt ?? DateTimeOffset.MaxValue)
                    .ThenBy(r => r.Id, StringComparer.Ordinal),
                (RunOrderBy.CompletedAt, false) => all
                    .OrderByDescending(r => r.CompletedAt ?? DateTimeOffset.MinValue)
                    .ThenByDescending(r => r.Id, StringComparer.Ordinal),
                (RunOrderBy.CompletedAt, true) => all
                    .OrderBy(r => r.CompletedAt ?? DateTimeOffset.MaxValue)
                    .ThenBy(r => r.Id, StringComparer.Ordinal),
                (_, false) => all
                    .OrderByDescending(r => r.CreatedAt)
                    .ThenByDescending(r => r.Id, StringComparer.Ordinal),
                (_, true) => all
                    .OrderBy(r => r.CreatedAt)
                    .ThenBy(r => r.Id, StringComparer.Ordinal)
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

    public Task<DurableSuspendOutcome> TrySuspendRunAsync(string runId, long expectedLeaseEpoch,
        IReadOnlyCollection<string> awaitedRunIds,
        IReadOnlyCollection<string> awaitedBatchIds,
        DateTimeOffset now,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (!_runs.TryGetValue(runId, out var stored))
            {
                return Task.FromResult(DurableSuspendOutcome.NotTransitioned);
            }

            if (stored.Status != JobStatus.Running || stored.LeaseEpoch != expectedLeaseEpoch)
            {
                return Task.FromResult(DurableSuspendOutcome.NotTransitioned);
            }

            // Check whether any awaited entity is still non-terminal. If yes, the orchestrator
            // parks in Suspended; the wake mechanism will replay it when the last awaited entity
            // terminates. If everything is already terminal, route straight to Pending so the
            // next claim sweep replays immediately (no wake source would ever arrive otherwise).
            var hasNonTerminal = false;
            foreach (var awaitedRunId in awaitedRunIds)
            {
                if (_runs.TryGetValue(awaitedRunId, out var awaited) && !awaited.Status.IsTerminal)
                {
                    hasNonTerminal = true;
                    break;
                }
            }

            if (!hasNonTerminal)
            {
                foreach (var awaitedBatchId in awaitedBatchIds)
                {
                    if (_batches.TryGetValue(awaitedBatchId, out var awaited) && !awaited.Status.IsTerminal)
                    {
                        hasNonTerminal = true;
                        break;
                    }
                }
            }

            var newStatus = hasNonTerminal ? JobStatus.Suspended : JobStatus.Pending;
            var oldStatus = stored.Status;
            var newStored = stored with
            {
                Status = newStatus,
                NodeName = null,
                NotBefore = hasNonTerminal ? stored.NotBefore : now,
                LastHeartbeatAt = now,
                ReplayCount = hasNonTerminal ? stored.ReplayCount : stored.ReplayCount + 1
            };
            _runs[runId] = newStored;

            UpdateIndexes(newStored, oldStatus, newStatus);
            AppendStatusEventCore(runId, newStored.Attempt, newStatus);

            if (hasNonTerminal)
            {
                // Persist the wait set: outgoing index (orchestrator -> awaited) and the inverse
                // (awaited -> orchestrators). Only non-terminal entities are stored; terminals
                // are observed inline above and won't drive a wake (they already happened).
                foreach (var awaitedRunId in awaitedRunIds)
                {
                    if (_runs.TryGetValue(awaitedRunId, out var awaited) && !awaited.Status.IsTerminal)
                    {
                        AddDurableWaitForRun(runId, awaitedRunId);
                    }
                }

                foreach (var awaitedBatchId in awaitedBatchIds)
                {
                    if (_batches.TryGetValue(awaitedBatchId, out var awaited) && !awaited.Status.IsTerminal)
                    {
                        AddDurableWaitForBatch(runId, awaitedBatchId);
                    }
                }
            }

            return Task.FromResult(hasNonTerminal
                ? DurableSuspendOutcome.Suspended
                : DurableSuspendOutcome.ImmediatePending);
        }
    }

    public Task<DurableExecutionSnapshot> LoadExecutionSnapshotAsync(string orchestratorRunId,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _runs.TryGetValue(orchestratorRunId, out var orchestrator);

            var children = new Dictionary<string, JobRun>(StringComparer.Ordinal);
            if (_childrenByParent.TryGetValue(orchestratorRunId, out var childIds))
            {
                foreach (var childId in childIds)
                {
                    if (_runs.TryGetValue(childId, out var child))
                    {
                        children[childId] = child;
                    }
                }
            }

            var childBatches = new Dictionary<string, JobBatch>(StringComparer.Ordinal);
            if (_batchesByParentRun.TryGetValue(orchestratorRunId, out var batchIds))
            {
                foreach (var batchId in batchIds)
                {
                    if (_batches.TryGetValue(batchId, out var batch))
                    {
                        childBatches[batchId] = batch;
                    }
                }
            }

            var records = _durableRecords
                .Where(kvp => string.Equals(kvp.Key.OrchestratorRunId, orchestratorRunId, StringComparison.Ordinal))
                .ToDictionary(kvp => kvp.Key.Step, kvp => kvp.Value);

            return Task.FromResult(new DurableExecutionSnapshot(
                children,
                childBatches,
                records,
                orchestrator?.HighestRecordedStep ?? 0));
        }
    }

    public Task<DurableRecord> CreateDurableRecordAsync(DurableRecord record,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var key = (record.OrchestratorRunId, record.Step);
            if (_durableRecords.TryGetValue(key, out var existing))
            {
                if (DurableRecordsEqual(existing, record))
                {
                    AdvanceHighestRecordedStep(record.OrchestratorRunId, record.Step);
                    return Task.FromResult(existing);
                }

                throw new DurableReplayMismatchException(record.OrchestratorRunId, record.Step,
                    $"Expected {DescribeRecord(record)}; saw {DescribeRecord(existing)}.");
            }

            _durableRecords[key] = record;
            AdvanceHighestRecordedStep(record.OrchestratorRunId, record.Step);

            return Task.FromResult(record);
        }
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

            if (stored.Status != transition.ExpectedStatus || stored.LeaseEpoch != transition.ExpectedLeaseEpoch)
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
                CanceledAt = transition.CanceledAt ?? stored.CanceledAt,
                Reason = transition.Reason,
                Result = transition.Result,
                Progress = transition.Progress,
                NotBefore = transition.NotBefore,
                LastHeartbeatAt = transition.LastHeartbeatAt ?? stored.LastHeartbeatAt,
                LeaseEpoch = transition.IncrementLeaseEpoch ? stored.LeaseEpoch + 1 : stored.LeaseEpoch,
                Attempt = transition.IncrementAttempt ? stored.Attempt + 1 : stored.Attempt,
                FailureCount = transition.IncrementFailureCount ? stored.FailureCount + 1 : stored.FailureCount
            };
            _runs[newStored.Id] = newStored;

            UpdateIndexes(newStored, oldStatus, newStatus);
            AppendStatusEventCore(newStored.Id, newStored.Attempt, newStored.Status);
            AppendEventsCore(transition.Events);

            if (newStatus.IsTerminal && newStored.DeduplicationId is { })
            {
                _dedupIndex.Remove((newStored.JobName, newStored.DeduplicationId));
            }

            var now = _timeProvider.GetUtcNow();
            var batchCompletion = newStatus.IsTerminal
                ? IncrementBatchCounter(newStored.BatchId, newStatus, now)
                : null;

            if (newStatus.IsTerminal)
            {
                WakeForTerminatedRun(newStored.Id, now);
                if (batchCompletion is { } bc)
                {
                    WakeForTerminatedBatch(bc.BatchId, now);
                }
            }

            return Task.FromResult(new RunTransitionResult(true, batchCompletion));
        }
    }

    public Task<RunTransitionResult> TryCancelRunAsync(string runId,
        long? expectedLeaseEpoch = null,
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

            if (expectedLeaseEpoch is { } epoch && stored.LeaseEpoch != epoch)
            {
                return Task.FromResult(RunTransitionResult.NotApplied);
            }

            var oldStatus = stored.Status;
            var CanceledAt = _timeProvider.GetUtcNow();
            var Canceled = stored with
            {
                Status = JobStatus.Canceled,
                CanceledAt = CanceledAt,
                CompletedAt = CanceledAt,
                Reason = reason
            };
            _runs[runId] = Canceled;

            UpdateIndexes(Canceled, oldStatus, JobStatus.Canceled);
            AppendStatusEventCore(Canceled.Id, Canceled.Attempt, Canceled.Status);
            AppendEventsCore(events);

            if (Canceled.DeduplicationId is { })
            {
                _dedupIndex.Remove((Canceled.JobName, Canceled.DeduplicationId));
            }

            var batchCompletion = IncrementBatchCounter(Canceled.BatchId, JobStatus.Canceled, CanceledAt);
            WakeForTerminatedRun(Canceled.Id, CanceledAt);
            if (batchCompletion is { } bc)
            {
                WakeForTerminatedBatch(bc.BatchId, CanceledAt);
            }

            return Task.FromResult(new RunTransitionResult(true, batchCompletion));
        }
    }

    public Task<SubtreeCancellation> CancelRunSubtreeAsync(string rootRunId,
        string? reason = null,
        bool includeRoot = true,
        CancellationToken cancellationToken = default)
        => Task.FromResult(CancelSubtreeCore(SubtreeSeed.Run, rootRunId, reason, includeRoot));

    public Task<SubtreeCancellation> CancelBatchSubtreeAsync(string batchId,
        string? reason = null,
        CancellationToken cancellationToken = default)
        => Task.FromResult(CancelSubtreeCore(SubtreeSeed.Batch, batchId, reason, true));

    public Task<IReadOnlyList<JobRun>> ClaimRunsAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, int maxCount, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxCount, 1);

        lock (_gate)
        {
            var now = _timeProvider.GetUtcNow();
            var jobNameSet = jobNames as ISet<string> ?? new HashSet<string>(jobNames);
            var queueNameSet = queueNames as ISet<string> ?? new HashSet<string>(queueNames);

            // Sort order matches the SQL/Redis stores: queue priority DESC, run priority DESC,
            // not_before ASC, id ASC.
            var candidates = new PendingCandidate[_pending.Count];
            var idx = 0;
            foreach (var (runId, entry) in _pending)
            {
                var queuePriority = _queues.TryGetValue(entry.QueueName, out var q) ? q.Priority : 0;
                candidates[idx++] = new(runId, entry.RunPriority, entry.NotBefore, queuePriority);
            }

            Array.Sort(candidates, PendingCandidate.ClaimOrder);

            // Counters and rate-limit state are updated in place so subsequent iterations see
            // the effect of earlier claims, preserving concurrency and rate-limit guarantees.
            var claimed = new List<JobRun>(Math.Min(maxCount, candidates.Length));

            foreach (var c in candidates)
            {
                if (claimed.Count >= maxCount)
                {
                    break;
                }

                if (!_runs.TryGetValue(c.RunId, out var run))
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

                if (run.LeaseEpoch == 0 && run.NotAfter is { } notAfter && notAfter < now)
                {
                    continue;
                }

                if (run.ExpiresAt is { } expiresAt && expiresAt < now)
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

                _pending.Remove(run.Id);
                DecrementCount(_pendingCountByQueue, queueName);
                var claimedRun = run with
                {
                    Status = JobStatus.Running,
                    NodeName = nodeName,
                    StartedAt = run.StartedAt ?? now,
                    LastHeartbeatAt = now,
                    LeaseEpoch = run.LeaseEpoch + 1
                };
                _runs[run.Id] = claimedRun;

                IncrementCount(_runningCountByJob, claimedRun.JobName);
                IncrementCount(_runningCountByQueue, queueName);
                AppendStatusEventCore(claimedRun.Id, claimedRun.Attempt, claimedRun.Status);

                claimed.Add(claimedRun);
            }

            return Task.FromResult<IReadOnlyList<JobRun>>(claimed);
        }
    }

    public Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        DurableStepRecord? durableStepRecord = null,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (_batches.ContainsKey(batch.Id))
            {
                throw new RunConflictException(batch.Id,
                    $"Batch '{batch.Id}' already exists.");
            }

            var copies = new List<JobRun>(runs.Count);
            foreach (var run in runs)
            {
                if (_runs.ContainsKey(run.Id))
                {
                    throw new RunConflictException(run.Id,
                        $"Run '{run.Id}' already exists.");
                }

                copies.Add(run);
            }

            _batches[batch.Id] = batch;
            if (batch.ParentRunId is { } parentRunId)
            {
                if (!_batchesByParentRun.TryGetValue(parentRunId, out var siblings))
                {
                    siblings = [];
                    _batchesByParentRun[parentRunId] = siblings;
                }

                siblings.Add(batch.Id);
            }

            foreach (var run in copies)
            {
                _runs[run.Id] = run;
                AddChildIndex(run);

                if (run.Status == JobStatus.Pending)
                {
                    AddToPendingIndex(run);
                    IncrementCount(_pendingCountByQueue, GetQueueName(run.JobName));
                }

                if (!run.Status.IsTerminal)
                {
                    IncrementCount(_nonTerminalCountByJob, run.JobName);
                }

                if (run.DeduplicationId is { })
                {
                    _dedupIndex.Add((run.JobName, run.DeduplicationId));
                }
            }

            // Atomically advance the orchestrator's HighestRecordedStep (monotonic max) for
            // durable orchestrators whose handler is creating this batch as a recorded step.
            if (durableStepRecord is { } step
                && _runs.TryGetValue(step.OrchestratorRunId, out var orchestrator)
                && step.Step > orchestrator.HighestRecordedStep)
            {
                _runs[step.OrchestratorRunId] = orchestrator with { HighestRecordedStep = step.Step };
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
            WakeForTerminatedBatch(batchId, completedAt);
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

            // take+1 lookahead so NextCursor is set iff a row exists beyond the page boundary.
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

    public Task<IReadOnlySet<string>> AppendEventsIfRunNonTerminalAsync(
        IReadOnlyList<RunEvent> events,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var acceptedRunIds = new HashSet<string>(StringComparer.Ordinal);
            foreach (var runId in events.Select(e => e.RunId).Distinct(StringComparer.Ordinal))
            {
                if (_runs.TryGetValue(runId, out var run) && !run.Status.IsTerminal)
                {
                    acceptedRunIds.Add(runId);
                }
            }

            if (acceptedRunIds.Count > 0)
            {
                AppendEventsCore(events.Where(e => acceptedRunIds.Contains(e.RunId)).ToList());
            }

            return Task.FromResult<IReadOnlySet<string>>(acceptedRunIds);
        }
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

    public Task UpsertQueuesAsync(IReadOnlyList<QueueDefinition> queues,
        CancellationToken cancellationToken = default)
    {
        if (queues.Count == 0)
        {
            return Task.CompletedTask;
        }

        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            foreach (var queue in queues)
            {
                var updated = CopyQueue(queue);
                updated.LastHeartbeatAt = now;
                if (_queues.TryGetValue(queue.Name, out var existing))
                {
                    updated.IsPaused = existing.IsPaused;
                }

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

    public Task<bool> SetQueuePausedAsync(string name, bool isPaused,
        CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            if (!_queues.TryGetValue(name, out var queue))
            {
                return Task.FromResult(false);
            }

            queue.IsPaused = isPaused;
            return Task.FromResult(true);
        }
    }

    public Task UpsertRateLimitsAsync(IReadOnlyList<RateLimitDefinition> rateLimits,
        CancellationToken cancellationToken = default)
    {
        if (rateLimits.Count == 0)
        {
            return Task.CompletedTask;
        }

        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            foreach (var rl in rateLimits)
            {
                var copy = CopyRateLimit(rl);
                copy.LastHeartbeatAt = now;
                _rateLimitDefinitions[rl.Name] = copy;
            }
        }

        return Task.CompletedTask;
    }

    public Task<SubtreeCancellation> CancelExpiredRunsWithIdsAsync(CancellationToken cancellationToken = default)
    {
        var now = _timeProvider.GetUtcNow();
        var canceledRuns = new List<CanceledRun>();
        var expiredRuns = new List<ExpiredCanceledRun>();
        var completedBatches = new List<BatchCompletionInfo>();

        lock (_gate)
        {
            var directExpiredIds = _runs.Values
                .Where(run => IsExpired(run, now))
                .Select(run => run.Id)
                .ToHashSet(StringComparer.Ordinal);
            if (directExpiredIds.Count == 0)
            {
                return Task.FromResult(SubtreeCancellation.Empty);
            }

            var expiredSeedIds = directExpiredIds
                .Where(id => !HasExpiredAncestor(id, directExpiredIds))
                .OrderBy(id => id, StringComparer.Ordinal)
                .ToHashSet(StringComparer.Ordinal);
            var queue = new Queue<string>(expiredSeedIds);
            var visited = new HashSet<string>(StringComparer.Ordinal);
            var terminatedBatchIds = new List<string>();

            while (queue.Count > 0)
            {
                var runId = queue.Dequeue();
                if (!visited.Add(runId))
                {
                    continue;
                }

                if (_childrenByParent.TryGetValue(runId, out var children))
                {
                    foreach (var childId in children.OrderBy(id => id, StringComparer.Ordinal))
                    {
                        queue.Enqueue(childId);
                    }
                }

                if (!_runs.TryGetValue(runId, out var run) || run.Status.IsTerminal)
                {
                    continue;
                }

                var oldStatus = run.Status;
                var isDirectExpiration = expiredSeedIds.Contains(run.Id);
                var reason = isDirectExpiration
                    ? "Run expired past its deadline."
                    : $"Canceled because parent run '{run.ParentRunId}' expired.";
                var canceled = run with
                {
                    Status = JobStatus.Canceled,
                    CanceledAt = now,
                    CompletedAt = now,
                    Reason = reason
                };
                _runs[run.Id] = canceled;

                if (canceled.DeduplicationId is { })
                {
                    _dedupIndex.Remove((canceled.JobName, canceled.DeduplicationId));
                }

                UpdateIndexes(canceled, oldStatus, JobStatus.Canceled);
                AppendStatusEventCore(canceled.Id, canceled.Attempt, canceled.Status);
                var batchCompletion = IncrementBatchCounter(canceled.BatchId, JobStatus.Canceled, now);
                WakeForTerminatedRun(canceled.Id, now);
                if (batchCompletion is { } bc)
                {
                    terminatedBatchIds.Add(bc.BatchId);
                    completedBatches.Add(bc);
                }

                canceledRuns.Add(new(canceled.Id, canceled.BatchId));
                expiredRuns.Add(new(
                    canceled.Id,
                    canceled.BatchId,
                    canceled.Attempt,
                    reason,
                    isDirectExpiration
                        ? ExpiredCancellationKind.Expired
                        : ExpiredCancellationKind.AncestorExpired));
            }

            terminatedBatchIds.Sort(StringComparer.Ordinal);
            foreach (var batchId in terminatedBatchIds)
            {
                WakeForTerminatedBatch(batchId, now);
            }
        }

        return Task.FromResult(canceledRuns.Count == 0 && completedBatches.Count == 0
            ? SubtreeCancellation.Empty
            : new(canceledRuns, completedBatches) { ExpiredRuns = expiredRuns });

        static bool IsExpired(JobRun run, DateTimeOffset now)
        {
            if (run.Status.IsTerminal)
            {
                return false;
            }

            var startExpired = run.Status == JobStatus.Pending
                               && run.LeaseEpoch == 0
                               && run.NotAfter is { } notAfter
                               && notAfter < now;
            var lifetimeExpired = run.ExpiresAt is { } expiresAt && expiresAt < now;
            return startExpired || lifetimeExpired;
        }

        bool HasExpiredAncestor(string runId, IReadOnlySet<string> directExpiredIds)
        {
            var currentId = runId;
            while (_runs.TryGetValue(currentId, out var current) && current.ParentRunId is { } parentId)
            {
                if (directExpiredIds.Contains(parentId))
                {
                    return true;
                }

                currentId = parentId;
            }

            return false;
        }
    }

    public Task PurgeAsync(DateTimeOffset threshold, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            var runsToRemove = _runs.Values
                .Where(r =>
                    CanPurgeTerminalRun(r, threshold) && r.CompletedAt < threshold)
                .Select(r => r.Id)
                .ToList();

            foreach (var id in runsToRemove)
            {
                if (_runs.Remove(id, out var removed))
                {
                    RemoveChildIndex(removed);
                    _pending.Remove(id);
                    if (_eventsByRunId.Remove(id, out var removedEvents))
                    {
                        RemoveBatchEventIndexes(removed, removedEvents);
                    }

                    foreach (var recordKey in _durableRecords.Keys
                                 .Where(k => string.Equals(k.OrchestratorRunId, id, StringComparison.Ordinal))
                                 .ToArray())
                    {
                        _durableRecords.Remove(recordKey);
                    }

                    var queueName = GetQueueName(removed.JobName);

                    if (removed.Status == JobStatus.Pending)
                    {
                        DecrementCount(_pendingCountByQueue, queueName);
                    }

                    if (removed.Status.ConsumesActiveSlot)
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
                .Where(b => !_runs.Values.Any(r => r.BatchId == b.Id))
                .Select(b => b.Id)
                .ToList();

            foreach (var id in batchesToRemove)
            {
                if (_batches.TryGetValue(id, out var purged)
                    && purged.ParentRunId is { } purgedParent
                    && _batchesByParentRun.TryGetValue(purgedParent, out var siblings))
                {
                    siblings.Remove(id);
                    if (siblings.Count == 0)
                    {
                        _batchesByParentRun.Remove(purgedParent);
                    }
                }

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

        var bucketCount = 0;
        var temp = sinceTime;
        while (temp < now)
        {
            bucketCount++;
            temp += bucketSpan;
        }

        var bucketPending = new int[bucketCount];
        var bucketRunning = new int[bucketCount];
        var bucketSuspended = new int[bucketCount];
        var bucketSucceeded = new int[bucketCount];
        var bucketCanceled = new int[bucketCount];
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
                else if (run.Status is JobStatus.Pending or JobStatus.Running)
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
                            case JobStatus.Suspended:
                                bucketSuspended[bucketIndex]++;
                                break;
                            case JobStatus.Succeeded:
                                bucketSucceeded[bucketIndex]++;
                                break;
                            case JobStatus.Canceled:
                                bucketCanceled[bucketIndex]++;
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
                Suspended = bucketSuspended[i],
                Succeeded = bucketSucceeded[i],
                Canceled = bucketCanceled[i],
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

    public Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(take, 1);

        lock (_gate)
        {
            var ids = _runs.Values
                .Where(r => r.Status == JobStatus.Running
                            && r.LastHeartbeatAt is { } hb
                            && hb < staleBefore)
                .OrderBy(r => r.LastHeartbeatAt!.Value)
                .ThenBy(r => r.Id, StringComparer.Ordinal)
                .Take(take)
                .Select(r => r.Id)
                .ToList();

            return Task.FromResult<IReadOnlyList<string>>(ids);
        }
    }

    private void AdvanceHighestRecordedStep(string orchestratorRunId, int step)
    {
        if (_runs.TryGetValue(orchestratorRunId, out var orchestrator)
            && step > orchestrator.HighestRecordedStep)
        {
            _runs[orchestratorRunId] = orchestrator with { HighestRecordedStep = step };
        }
    }


    // --- Durable wait table (in-process model) ---

    private void AddDurableWaitForRun(string orchestratorId, string awaitedRunId)
    {
        if (!_durableWaitsByOrchestratorRun.TryGetValue(orchestratorId, out var outgoing))
        {
            outgoing = new(StringComparer.Ordinal);
            _durableWaitsByOrchestratorRun[orchestratorId] = outgoing;
        }

        outgoing.Add(awaitedRunId);

        if (!_orchestratorsAwaitingRun.TryGetValue(awaitedRunId, out var incoming))
        {
            incoming = new(StringComparer.Ordinal);
            _orchestratorsAwaitingRun[awaitedRunId] = incoming;
        }

        incoming.Add(orchestratorId);
    }

    private void AddDurableWaitForBatch(string orchestratorId, string awaitedBatchId)
    {
        if (!_durableWaitsByOrchestratorBatch.TryGetValue(orchestratorId, out var outgoing))
        {
            outgoing = new(StringComparer.Ordinal);
            _durableWaitsByOrchestratorBatch[orchestratorId] = outgoing;
        }

        outgoing.Add(awaitedBatchId);

        if (!_orchestratorsAwaitingBatch.TryGetValue(awaitedBatchId, out var incoming))
        {
            incoming = new(StringComparer.Ordinal);
            _orchestratorsAwaitingBatch[awaitedBatchId] = incoming;
        }

        incoming.Add(orchestratorId);
    }

    /// <summary>
    ///     Removes every wait row owned by <paramref name="orchestratorId" /> (step 1 of the wake
    ///     mechanism: outgoing cleanup). No-op for non-Suspended runs; required for cancel paths
    ///     where a Suspended -> Canceled run still has outgoing wait rows.
    /// </summary>
    private void RemoveOutgoingDurableWaits(string orchestratorId)
    {
        if (_durableWaitsByOrchestratorRun.Remove(orchestratorId, out var outgoingRuns))
        {
            foreach (var awaitedRunId in outgoingRuns)
            {
                if (_orchestratorsAwaitingRun.TryGetValue(awaitedRunId, out var incoming))
                {
                    incoming.Remove(orchestratorId);
                    if (incoming.Count == 0)
                    {
                        _orchestratorsAwaitingRun.Remove(awaitedRunId);
                    }
                }
            }
        }

        if (_durableWaitsByOrchestratorBatch.Remove(orchestratorId, out var outgoingBatches))
        {
            foreach (var awaitedBatchId in outgoingBatches)
            {
                if (_orchestratorsAwaitingBatch.TryGetValue(awaitedBatchId, out var incoming))
                {
                    incoming.Remove(orchestratorId);
                    if (incoming.Count == 0)
                    {
                        _orchestratorsAwaitingBatch.Remove(awaitedBatchId);
                    }
                }
            }
        }
    }

    /// <summary>
    ///     Step 2 + 3 of the wake mechanism. Deletes every incoming wait row referencing
    ///     <paramref name="terminatedId" /> (interpreted as a run id or batch id per
    ///     <paramref name="kind" />), and for each affected orchestrator whose wait set is now
    ///     empty, transitions <see cref="JobStatus.Suspended" /> -> <see cref="JobStatus.Pending" />.
    /// </summary>
    private void WakeOrchestratorsAwaiting(string terminatedId, DurableWaitKind kind, DateTimeOffset now)
    {
        var incomingIndex = kind == DurableWaitKind.Run ? _orchestratorsAwaitingRun : _orchestratorsAwaitingBatch;
        if (!incomingIndex.Remove(terminatedId, out var affectedOrchestrators) ||
            affectedOrchestrators.Count == 0)
        {
            return;
        }

        // Deterministic id order matches the SQL stores' sorted-id locking discipline; the
        // in-process gate already serializes here, but ordered iteration keeps debug traces
        // stable across stores.
        var orderedAffected = affectedOrchestrators.ToArray();
        Array.Sort(orderedAffected, StringComparer.Ordinal);

        foreach (var orchestratorId in orderedAffected)
        {
            // Step 2: clean the orchestrator's outgoing edge for this awaited entity.
            var outgoingIndex = kind == DurableWaitKind.Run
                ? _durableWaitsByOrchestratorRun
                : _durableWaitsByOrchestratorBatch;
            if (outgoingIndex.TryGetValue(orchestratorId, out var outgoing))
            {
                outgoing.Remove(terminatedId);
                if (outgoing.Count == 0)
                {
                    outgoingIndex.Remove(orchestratorId);
                }
            }

            // Step 3: wake if the orchestrator's combined wait set is now empty.
            var runsRemain = _durableWaitsByOrchestratorRun.TryGetValue(orchestratorId, out var rem1) &&
                             rem1.Count > 0;
            var batchesRemain = _durableWaitsByOrchestratorBatch.TryGetValue(orchestratorId, out var rem2) &&
                                rem2.Count > 0;
            if (runsRemain || batchesRemain)
            {
                continue;
            }

            if (!_runs.TryGetValue(orchestratorId, out var orchestrator) ||
                orchestrator.Status != JobStatus.Suspended)
            {
                continue;
            }

            var waked = orchestrator with
            {
                Status = JobStatus.Pending,
                NotBefore = now,
                LastHeartbeatAt = now,
                ReplayCount = orchestrator.ReplayCount + 1
            };
            _runs[orchestratorId] = waked;
            UpdateIndexes(waked, JobStatus.Suspended, JobStatus.Pending);
            AppendStatusEventCore(orchestratorId, waked.Attempt, JobStatus.Pending);
        }
    }

    /// <summary>
    ///     Convenience: runs all three wake steps for a just-terminated run id. Step 1 clears the
    ///     terminated run's outgoing waits (matters when a Suspended run is canceled mid-wait);
    ///     steps 2 + 3 wake any orchestrator awaiting this run id whose wait set is now empty.
    /// </summary>
    private void WakeForTerminatedRun(string terminatedRunId, DateTimeOffset now)
    {
        RemoveOutgoingDurableWaits(terminatedRunId);
        WakeOrchestratorsAwaiting(terminatedRunId, DurableWaitKind.Run, now);
    }

    private void WakeForTerminatedBatch(string terminatedBatchId, DateTimeOffset now) =>
        WakeOrchestratorsAwaiting(terminatedBatchId, DurableWaitKind.Batch, now);

    private SubtreeCancellation CancelSubtreeCore(SubtreeSeed seed, string seedId, string? reason,
        bool includeRoot)
    {
        var canceledRuns = new List<CanceledRun>();
        var completedBatches = new List<BatchCompletionInfo>();
        var now = _timeProvider.GetUtcNow();

        lock (_gate)
        {
            switch (seed)
            {
                case SubtreeSeed.Run when !_runs.ContainsKey(seedId):
                    return SubtreeCancellation.NotFound;
                case SubtreeSeed.Batch when !_batches.ContainsKey(seedId):
                    return SubtreeCancellation.NotFound;
            }

            var queue = new Queue<string>();
            var visited = new HashSet<string>(StringComparer.Ordinal);

            void Enqueue(string id)
            {
                if (visited.Add(id))
                {
                    queue.Enqueue(id);
                }
            }

            switch (seed)
            {
                case SubtreeSeed.Run:
                    if (includeRoot)
                    {
                        Enqueue(seedId);
                    }
                    else if (_childrenByParent.TryGetValue(seedId, out var directChildren))
                    {
                        foreach (var c in directChildren)
                        {
                            Enqueue(c);
                        }
                    }

                    break;
                case SubtreeSeed.Batch:
                    foreach (var run in _runs.Values)
                    {
                        if (run.BatchId == seedId)
                        {
                            Enqueue(run.Id);
                        }
                    }

                    break;
            }

            var canceledSet = new HashSet<string>(StringComparer.Ordinal);
            var terminatedBatchIds = new List<string>();

            while (queue.Count > 0)
            {
                var currentId = queue.Dequeue();

                if (_childrenByParent.TryGetValue(currentId, out var children))
                {
                    foreach (var c in children)
                    {
                        Enqueue(c);
                    }
                }

                if (!_runs.TryGetValue(currentId, out var run) || run.Status.IsTerminal)
                {
                    continue;
                }

                var oldStatus = run.Status;
                var canceled = run with
                {
                    Status = JobStatus.Canceled,
                    CanceledAt = now,
                    CompletedAt = now,
                    Reason = reason
                };
                _runs[currentId] = canceled;

                UpdateIndexes(canceled, oldStatus, JobStatus.Canceled);
                AppendStatusEventCore(canceled.Id, canceled.Attempt, canceled.Status);

                if (canceled.DeduplicationId is { })
                {
                    _dedupIndex.Remove((canceled.JobName, canceled.DeduplicationId));
                }

                var batchCompletion = IncrementBatchCounter(canceled.BatchId, JobStatus.Canceled, now);
                if (batchCompletion is { } bc)
                {
                    completedBatches.Add(bc);
                    terminatedBatchIds.Add(bc.BatchId);
                }

                canceledSet.Add(canceled.Id);
                canceledRuns.Add(new(canceled.Id, canceled.BatchId));
            }

            // Three-step wake per terminated entity, in deterministic id order to match the SQL
            // stores' sorted-id locking. Each canceled run might be itself awaited by another
            // orchestrator (the canceled run's incoming waits get cleared and any orchestrator
            // whose wait set is now empty is transitioned Suspended -> Pending). Outgoing waits
            // (step 1) handle the case where a Suspended -> Canceled run still had outgoing rows.
            var orderedRunIds = canceledSet.ToArray();
            Array.Sort(orderedRunIds, StringComparer.Ordinal);
            foreach (var canceledRunId in orderedRunIds)
            {
                WakeForTerminatedRun(canceledRunId, now);
            }

            terminatedBatchIds.Sort(StringComparer.Ordinal);
            foreach (var bid in terminatedBatchIds)
            {
                WakeForTerminatedBatch(bid, now);
            }
        }

        return canceledRuns.Count == 0 && completedBatches.Count == 0
            ? SubtreeCancellation.Empty
            : new(canceledRuns, completedBatches);
    }

    private void AddChildIndex(JobRun run)
    {
        if (run.ParentRunId is not { } parentId)
        {
            return;
        }

        if (!_childrenByParent.TryGetValue(parentId, out var list))
        {
            list = [];
            _childrenByParent[parentId] = list;
        }

        list.Add(run.Id);
    }

    private void RemoveChildIndex(JobRun run)
    {
        if (run.ParentRunId is not { } parentId)
        {
            return;
        }

        if (_childrenByParent.TryGetValue(parentId, out var list))
        {
            list.Remove(run.Id);
            if (list.Count == 0)
            {
                _childrenByParent.Remove(parentId);
            }
        }
    }

    private static DateTimeOffset GetTimelineBucketTimestamp(JobRun run) => run.Status switch
    {
        JobStatus.Running => run.StartedAt ?? run.CreatedAt,
        JobStatus.Succeeded => run.CompletedAt ?? run.StartedAt ?? run.CreatedAt,
        JobStatus.Canceled => run.CanceledAt ?? run.CompletedAt ?? run.CreatedAt,
        JobStatus.Failed => run.CompletedAt ?? run.StartedAt ?? run.CreatedAt,
        _ => run.CreatedAt
    };

    private Task CreateRunsAsyncCore(IReadOnlyList<JobRun> runs, IReadOnlyList<RunEvent>? initialEvents)
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
                _runs[run.Id] = run;
                AddChildIndex(run);

                if (run.Status == JobStatus.Pending)
                {
                    AddToPendingIndex(run);
                    IncrementCount(_pendingCountByQueue, GetQueueName(run.JobName));
                }
                else if (run.Status.ConsumesActiveSlot)
                {
                    IncrementCount(_runningCountByJob, run.JobName);
                    IncrementCount(_runningCountByQueue, GetQueueName(run.JobName));
                }

                if (run.DeduplicationId is { })
                {
                    _dedupIndex.Add((run.JobName, run.DeduplicationId));
                }

                if (!run.Status.IsTerminal)
                {
                    IncrementCount(_nonTerminalCountByJob, run.JobName);
                }
            }

            AppendEventsCore(initialEvents);
        }

        return Task.CompletedTask;
    }

    private Task<bool> TryCreateRunAsyncCore(JobRun run, int? maxActiveForJob,
        DateTimeOffset? lastCronFireAt,
        IReadOnlyList<RunEvent>? initialEvents,
        DurableStepRecord? durableStepRecord = null)
    {
        lock (_gate)
        {
            if (_runs.ContainsKey(run.Id))
            {
                return Task.FromResult(false);
            }

            if (run.DeduplicationId is { }
                && _dedupIndex.Contains((run.JobName, run.DeduplicationId)))
            {
                throw new RunConflictException(run.Id,
                    $"Run with deduplication id '{run.DeduplicationId}' is already active for job '{run.JobName}'.");
            }

            if (maxActiveForJob is { })
            {
                _nonTerminalCountByJob.TryGetValue(run.JobName, out var activeCount);

                if (activeCount >= maxActiveForJob.Value)
                {
                    throw new RunConflictException(run.Id,
                        $"Job '{run.JobName}' is at the maximum active run capacity ({maxActiveForJob.Value}).");
                }
            }

            _runs[run.Id] = run;
            AddChildIndex(run);

            if (run.Status == JobStatus.Pending)
            {
                AddToPendingIndex(run);
                IncrementCount(_pendingCountByQueue, GetQueueName(run.JobName));
            }
            else if (run.Status.ConsumesActiveSlot)
            {
                IncrementCount(_runningCountByJob, run.JobName);
                IncrementCount(_runningCountByQueue, GetQueueName(run.JobName));
            }

            if (run.DeduplicationId is { })
            {
                _dedupIndex.Add((run.JobName, run.DeduplicationId));
            }

            if (!run.Status.IsTerminal)
            {
                IncrementCount(_nonTerminalCountByJob, run.JobName);
            }

            if (lastCronFireAt is { } fireAt && _jobs.TryGetValue(run.JobName, out var jobDef))
            {
                jobDef.LastCronFireAt = fireAt;
            }

            // Atomically advance the orchestrator's HighestRecordedStep (monotonic max) for
            // durable orchestrators whose handler is creating this run as a recorded step.
            if (durableStepRecord is { } step
                && _runs.TryGetValue(step.OrchestratorRunId, out var orchestrator)
                && step.Step > orchestrator.HighestRecordedStep)
            {
                _runs[step.OrchestratorRunId] = orchestrator with { HighestRecordedStep = step.Step };
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

    private static bool DurableRecordsEqual(DurableRecord left, DurableRecord right) =>
        string.Equals(left.OrchestratorRunId, right.OrchestratorRunId, StringComparison.Ordinal)
        && left.Step == right.Step
        && string.Equals(left.Kind, right.Kind, StringComparison.Ordinal)
        && string.Equals(left.Name, right.Name, StringComparison.Ordinal)
        && string.Equals(left.Payload, right.Payload, StringComparison.Ordinal);

    private static string DescribeRecord(DurableRecord record) =>
        record.Name is { Length: > 0 }
            ? $"record '{record.Name}' ({record.Kind})"
            : $"record kind '{record.Kind}'";

    private bool CanPurgeTerminalRun(JobRun run, DateTimeOffset threshold)
    {
        if (!run.Status.IsTerminal || run.CompletedAt is null)
        {
            return false;
        }

        if (run.BatchId is null)
        {
            return !TreeHasOpenRun(run);
        }

        if (!_batches.TryGetValue(run.BatchId, out var batch))
        {
            return true;
        }

        return !TreeHasOpenRun(run)
               && batch.Status.IsTerminal
               && batch.CompletedAt is { } batchCompletedAt
               && batchCompletedAt < threshold;
    }

    private bool TreeHasOpenRun(JobRun run)
    {
        var rootId = run.RootRunId ?? run.Id;
        return _runs.Values.Any(r => (r.RootRunId ?? r.Id) == rootId && !r.Status.IsTerminal);
    }

    private void AddToPendingIndex(JobRun run)
    {
        _pending[run.Id] = new(GetQueueName(run.JobName), run.Priority, run.NotBefore);
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
            _pending.Remove(run.Id);
            DecrementCount(_pendingCountByQueue, queueName);
        }

        if (oldStatus.ConsumesActiveSlot)
        {
            DecrementCount(_runningCountByJob, run.JobName);
            DecrementCount(_runningCountByQueue, queueName);
        }

        if (newStatus == JobStatus.Pending)
        {
            AddToPendingIndex(run);
            IncrementCount(_pendingCountByQueue, queueName);
        }

        if (newStatus.ConsumesActiveSlot)
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

    private void AppendStatusEventCore(string runId, int attempt, JobStatus status) =>
        AppendEventsCore([RunStatusEvents.Create(runId, attempt, status, _timeProvider.GetUtcNow())]);

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

    // FixedWindow: count resets when the window elapses.
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
        SourceCode = job.SourceCode,
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
            JobStatus.Canceled => batch with { Canceled = batch.Canceled + 1 },
            _ => batch
        };

        BatchCompletionInfo? batchCompletion = null;
        if (batch.Succeeded + batch.Failed + batch.Canceled >= batch.Total)
        {
            var batchStatus = batch.Failed > 0 ? JobStatus.Failed
                : batch.Canceled > 0 ? JobStatus.Canceled
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

    private enum DurableWaitKind
    {
        Run,
        Batch
    }

    private enum SubtreeSeed
    {
        Run,
        Batch
    }

    private sealed class RateLimitWindowState
    {
        public int CurrentCount;
        public int PreviousCount;
        public DateTimeOffset WindowStart;
    }

    private readonly record struct PendingRunEntry(string QueueName, int RunPriority, DateTimeOffset NotBefore);

    // Transient claim-time snapshot. Queue priority is looked up once per claim from _queues
    // rather than stored on the entry so runtime priority changes apply on the very next claim.
    private readonly record struct PendingCandidate(
        string RunId,
        int RunPriority,
        DateTimeOffset NotBefore,
        int QueuePriority)
    {
        public static readonly IComparer<PendingCandidate> ClaimOrder = new Comparer();

        private sealed class Comparer : IComparer<PendingCandidate>
        {
            public int Compare(PendingCandidate x, PendingCandidate y)
            {
                var cmp = y.QueuePriority.CompareTo(x.QueuePriority);
                if (cmp != 0)
                {
                    return cmp;
                }

                cmp = y.RunPriority.CompareTo(x.RunPriority);
                if (cmp != 0)
                {
                    return cmp;
                }

                cmp = x.NotBefore.CompareTo(y.NotBefore);
                if (cmp != 0)
                {
                    return cmp;
                }

                return string.CompareOrdinal(x.RunId, y.RunId);
            }
        }
    }
}
