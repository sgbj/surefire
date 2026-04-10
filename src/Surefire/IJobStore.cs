namespace Surefire;

/// <summary>
///     Defines the storage contract for Surefire job data. All implementations must provide
///     equivalent behavioral and atomicity guarantees as specified in the store contract.
/// </summary>
internal interface IJobStore
{
    /// <summary>
    ///     Creates or updates the store schema. This is always the first method called on the store,
    ///     invoked once at startup by <c>SurefireInitializationService</c>.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task MigrateAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Performs a lightweight connectivity probe against the underlying store.
    ///     Implementations should avoid heavyweight scans.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task PingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    /// <summary>
    ///     Inserts or updates a job definition. If a job with the same name exists, all mutable
    ///     fields are overwritten except <c>IsEnabled</c> and <c>LastCronFireAt</c>, which are
    ///     preserved across upserts.
    /// </summary>
    /// <param name="job">The job definition to upsert.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the job definition with the specified name, or null if not found.
    /// </summary>
    /// <param name="name">The job name.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching job definition, or null.</returns>
    Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns all job definitions matching the optional filter.
    /// </summary>
    /// <param name="filter">Optional filter criteria. When null, all jobs are returned.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A list of matching job definitions.</returns>
    Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Enables or disables a job. This is a user-facing toggle (dashboard) that survives
    ///     upsert re-sync. A no-op if the job does not exist.
    /// </summary>
    /// <param name="name">The job name.</param>
    /// <param name="enabled">True to enable, false to disable.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Updates the last cron fire time for a job. Used to track which cron occurrences have
    ///     been fired across nodes and restarts.
    /// </summary>
    /// <param name="jobName">The job name.</param>
    /// <param name="fireAt">The fire time to record.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Inserts one or more runs in a single atomic transaction. All runs are inserted or none.
    ///     When <paramref name="initialEvents" /> is provided, runs and events are persisted atomically
    ///     in the same transaction/critical section.
    /// </summary>
    /// <param name="runs">The runs to insert.</param>
    /// <param name="initialEvents">Optional events to append atomically with run creation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task CreateRunsAsync(IReadOnlyList<RunRecord> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Attempts to create a single run with optional deduplication, concurrency checks,
    ///     and cron fire tracking. All checks, the insert, and optional initial event append
    ///     are a single atomic operation. Returns false when validation fails (for example,
    ///     duplicate active deduplication ID or concurrency limit reached).
    /// </summary>
    /// <param name="run">The run to create.</param>
    /// <param name="maxActiveForJob">
    ///     When non-null, the run is rejected if the number of non-terminal runs for the same job
    ///     is already at or above this value. Used for continuous job seeding.
    /// </param>
    /// <param name="lastCronFireAt">
    ///     When non-null, atomically updates the job's <c>LastCronFireAt</c> to this value
    ///     as part of the same transaction that creates the run. Used by the cron scheduler
    ///     to prevent duplicate fires across restarts and multi-node deployments.
    /// </param>
    /// <param name="initialEvents">Optional events to append atomically with run creation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>True if the run was created; false if it was rejected.</returns>
    Task<bool> TryCreateRunAsync(RunRecord run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the run with the specified ID, or null if not found.
    /// </summary>
    /// <param name="id">The run ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching run, or null.</returns>
    Task<RunRecord?> GetRunAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns a page of runs matching the specified filter criteria.
    /// </summary>
    /// <param name="filter">The filter criteria.</param>
    /// <param name="skip">The number of matching items to skip (offset-based pagination). Must be greater than or equal to 0.</param>
    /// <param name="take">The maximum number of items to return. Must be greater than 0.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A paged result containing the matching runs and total count.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     <paramref name="skip" /> is less than 0, or <paramref name="take" /> is
    ///     less than or equal to 0.
    /// </exception>
    Task<PagedResult<RunRecord>> GetRunsAsync(RunFilter filter, int skip = 0, int take = 50,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Updates mutable fields on a run (progress, result, error, trace IDs, heartbeat).
    ///     Fenced by node ownership: the update is silently skipped if the run's current
    ///     <c>NodeName</c> does not match the run's <c>NodeName</c> property, or if the run
    ///     is already in a terminal status.
    /// </summary>
    /// <param name="run">The run with updated fields. <c>NodeName</c> is used as a fencing token.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task UpdateRunAsync(RunRecord run, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Atomically applies a run status transition using compare-and-swap. The transition succeeds only
    ///     if the run's current status and attempt match the expected values. Exactly one concurrent
    ///     caller wins. Timestamp fields supplied as null on <paramref name="transition" /> preserve the
    ///     existing stored values.
    /// </summary>
    /// <param name="transition">The transition request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>True if the transition was applied; false if the current status or attempt did not match.</returns>
    Task<bool> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Atomically cancels a run if it is not already in a terminal status. Unlike
    ///     <see cref="TryTransitionRunAsync" />, this does not require a specific expected status --
    ///     it transitions from any non-terminal status to Cancelled.
    /// </summary>
    /// <param name="runId">The run ID to cancel.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>True if the run was cancelled; false if it was already terminal.</returns>
    Task<bool> TryCancelRunAsync(string runId,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Cancels all non-terminal runs belonging to the specified parent run.
    ///     Returns the IDs of runs that were transitioned to <see cref="JobStatus.Cancelled"/>.
    /// </summary>
    /// <param name="parentRunId">The parent run ID whose children should be cancelled.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The IDs of child runs that were cancelled.</returns>
    async Task<IReadOnlyList<string>> CancelChildRunsAsync(string parentRunId,
        CancellationToken cancellationToken = default)
    {
        var cancelledIds = new List<string>();
        const int take = 200;
        while (true)
        {
            // Always query from skip=0: cancelled items become terminal and fall out of
            // the IsTerminal=false filter, so the result set naturally shrinks each iteration.
            var page = await GetRunsAsync(
                new() { ParentRunId = parentRunId, IsTerminal = false, OrderBy = RunOrderBy.CreatedAt },
                0, take, cancellationToken);
            if (page.Items.Count == 0) break;
            foreach (var child in page.Items)
            {
                if (await TryCancelRunAsync(child.Id, cancellationToken))
                {
                    cancelledIds.Add(child.Id);
                }
            }
        }
        return cancelledIds;
    }

    /// <summary>
    ///     Atomically finds and claims the highest-priority pending run that matches the node's
    ///     registered jobs and queues. The claim operation verifies max concurrency (job and queue),
    ///     rate limits, and queue pause status in a single atomic operation. The run's attempt is
    ///     incremented as part of the claim.
    /// </summary>
    /// <param name="nodeName">The name of the claiming node.</param>
    /// <param name="jobNames">The job names this node is registered to process.</param>
    /// <param name="queueNames">The queue names this node is registered to process.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The claimed run with updated status, node, and attempt; or null if no eligible run was found.</returns>
    Task<RunRecord?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Inserts a batch record and its child runs atomically. The batch starts with
    ///     <see cref="JobStatus.Pending" /> status and transitions when runs complete.
    /// </summary>
    /// <param name="batch">The batch record to create.</param>
    /// <param name="runs">The child runs belonging to the batch.</param>
    /// <param name="initialEvents">Optional events to append atomically with batch and run creation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task CreateBatchAsync(BatchRecord batch, IReadOnlyList<RunRecord> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the batch record with the specified ID, or null if not found.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching batch record, or null.</returns>
    Task<BatchRecord?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the IDs of all runs belonging to the specified batch.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The run IDs belonging to the batch.</returns>
    async Task<IReadOnlyList<string>> GetBatchRunIdsAsync(string batchId,
        CancellationToken cancellationToken = default)
    {
        var ids = new List<string>();
        var skip = 0;
        const int pageSize = 200;
        while (true)
        {
            var page = await GetRunsAsync(new() { BatchId = batchId }, skip, pageSize, cancellationToken);
            foreach (var run in page.Items)
                ids.Add(run.Id);
            if (page.Items.Count < pageSize)
                break;
            skip += page.Items.Count;
        }
        return ids;
    }

    /// <summary>
    ///     Atomically increments the batch progress counters and returns the post-increment values.
    ///     Returns <c>null</c> when the batch does not exist. The increment is skipped (returning
    ///     current values) if the batch is already in a terminal status.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="terminalStatus">The terminal status of the completed run.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The post-increment batch counters, or null when the batch does not exist.</returns>
    Task<BatchCounters?> TryIncrementBatchProgressAsync(string batchId, JobStatus terminalStatus,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Atomically transitions a batch from a non-terminal status to the specified terminal status.
    ///     Returns true if the transition was applied; false if already terminal.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="status">The terminal status to transition to.</param>
    /// <param name="completedAt">The completion timestamp to record.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>True if the transition was applied; false if the batch was already terminal.</returns>
    Task<bool> TryCompleteBatchAsync(string batchId, JobStatus status, DateTimeOffset completedAt,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Cancels all non-terminal runs belonging to the specified batch.
    ///     Returns the IDs of runs that were transitioned to <see cref="JobStatus.Cancelled"/>.
    /// </summary>
    /// <param name="batchId">The batch ID whose runs should be cancelled.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The IDs of runs that were cancelled.</returns>
    async Task<IReadOnlyList<string>> CancelBatchRunsAsync(string batchId,
        CancellationToken cancellationToken = default)
    {
        var cancelledIds = new List<string>();
        const int take = 200;
        while (true)
        {
            var page = await GetRunsAsync(
                new() { BatchId = batchId, IsTerminal = false, OrderBy = RunOrderBy.CreatedAt },
                0, take, cancellationToken);
            if (page.Items.Count == 0) break;
            foreach (var child in page.Items)
            {
                if (await TryCancelRunAsync(child.Id, cancellationToken))
                    cancelledIds.Add(child.Id);
            }
        }
        return cancelledIds;
    }

    /// <summary>
    ///     Appends one or more events to the event log. Events are immutable once written.
    /// </summary>
    /// <param name="events">The events to append.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns events for a run, optionally filtered by cursor position, event type, and attempt.
    /// </summary>
    /// <param name="runId">The run ID.</param>
    /// <param name="sinceId">Only return events with an ID greater than this value (cursor-based pagination).</param>
    /// <param name="types">When non-null, only return events of these types.</param>
    /// <param name="attempt">
    ///     When non-null, only return events for this attempt number. Events with
    ///     <c>Attempt = 0</c> (run-scoped, shared across attempts) are always included.
    /// </param>
    /// <param name="take">When non-null, limits the number of events returned. Must be greater than 0.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching events ordered by ID ascending.</returns>
    Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0, RunEventType[]? types = null,
        int? attempt = null, int? take = null, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns output events across all direct child runs of a batch coordinator, ordered by
    ///     event ID ascending for global cross-child event ordering.
    /// </summary>
    /// <param name="batchRunId">The batch coordinator run ID.</param>
    /// <param name="sinceEventId">Only return events with an ID greater than this value.</param>
    /// <param name="take">The maximum number of events to return.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching output events ordered by ID ascending.</returns>
    Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchRunId, long sinceEventId = 0, int take = 200,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Upserts the node record and batch-updates heartbeat timestamps on the node's active runs.
    ///     Called on every maintenance tick as the combined heartbeat operation.
    /// </summary>
    /// <param name="nodeName">The node name.</param>
    /// <param name="jobNames">The job names registered on this node.</param>
    /// <param name="queueNames">The queue names registered on this node.</param>
    /// <param name="activeRunIds">The IDs of runs currently executing on this node.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames, IReadOnlyCollection<string> queueNames,
        IReadOnlyCollection<string> activeRunIds, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns all known nodes, including inactive ones that have not been purged.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>All node records.</returns>
    Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns a specific node by name, or null when not found.
    /// </summary>
    /// <param name="name">The node name.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching node record, or null.</returns>
    async Task<NodeInfo?> GetNodeAsync(string name, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var nodes = await GetNodesAsync(cancellationToken);
        return nodes.FirstOrDefault(node => string.Equals(node.Name, name, StringComparison.Ordinal));
    }

    /// <summary>
    ///     Inserts or updates a queue definition. If a queue with the same name exists, all mutable
    ///     fields are overwritten except <c>IsPaused</c>, which is preserved across upserts.
    /// </summary>
    /// <param name="queue">The queue definition to upsert.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns all queue definitions.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>All queue definitions.</returns>
    Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Pauses or unpauses a queue. When paused, no runs from the queue are claimed.
    ///     A no-op if the queue does not exist.
    /// </summary>
    /// <param name="name">The queue name.</param>
    /// <param name="isPaused">True to pause, false to unpause.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Inserts or updates a rate limit definition. Does not reset runtime counters on update.
    ///     Rate limit acquisition is embedded inside <see cref="ClaimRunAsync" /> -- there is no
    ///     standalone acquire method.
    /// </summary>
    /// <param name="rateLimit">The rate limit definition to upsert.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Cancels all pending or retrying runs whose <c>NotAfter</c> deadline has passed.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The number of runs that were cancelled.</returns>
    Task<int> CancelExpiredRunsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Cancels all pending or retrying runs whose <c>NotAfter</c> deadline has passed and
    ///     returns the run IDs that were transitioned to <see cref="JobStatus.Cancelled"/>.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The IDs of runs that were cancelled in this operation.</returns>
    async Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(CancellationToken cancellationToken = default)
    {
        await CancelExpiredRunsAsync(cancellationToken);
        return [];
    }

    /// <summary>
    ///     Purges all data older than the specified threshold: terminal runs (by <c>CompletedAt</c>)
    ///     and their events, abandoned pending/retrying runs (by <c>NotBefore</c>),
    ///     and stale jobs, queues, rate limits, and nodes (by <c>LastHeartbeatAt</c>).
    /// </summary>
    /// <param name="threshold">The cutoff time. Items older than this are purged.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task PurgeAsync(DateTimeOffset threshold, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns aggregate statistics for the dashboard, including status counts and a timeline
    ///     of runs bucketed by status and time interval.
    /// </summary>
    /// <param name="since">The start of the time range for timeline buckets. When null, a store-specific default is used.</param>
    /// <param name="bucketMinutes">The width of each timeline bucket in minutes.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The dashboard statistics.</returns>
    Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns statistics for a specific job, including status counts and recent run history.
    /// </summary>
    /// <param name="jobName">The job name.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The job statistics.</returns>
    Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns statistics for all queues, including depth and throughput metrics,
    ///     keyed by queue name.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A dictionary of queue statistics keyed by queue name.</returns>
    Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(CancellationToken cancellationToken = default);
}