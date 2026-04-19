namespace Surefire;

/// <summary>
///     Defines the storage contract for Surefire job data. All implementations must provide
///     equivalent behavioral and atomicity guarantees as specified in the store contract.
/// </summary>
public interface IJobStore
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
    Task CreateRunsAsync(IReadOnlyList<JobRun> runs,
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
    Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the run with the specified ID, or null if not found.
    /// </summary>
    /// <param name="id">The run ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching run, or null.</returns>
    Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns runs for the specified IDs in a single bulk fetch. Runs are returned in the
    ///     same order as <paramref name="ids" />; missing runs are omitted. Implementations MUST
    ///     issue a single round trip (Redis MGET, SQL <c>WHERE id IN (...)</c>, or equivalent).
    /// </summary>
    /// <param name="ids">The run IDs to fetch.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching runs, in the order of <paramref name="ids" />, with missing IDs omitted.</returns>
    Task<IReadOnlyList<JobRun>> GetRunsByIdsAsync(IReadOnlyList<string> ids,
        CancellationToken cancellationToken = default);

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
    Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, int skip = 0, int take = 50,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Updates mutable fields on a run (progress, result, reason, trace IDs, heartbeat).
    ///     Fenced by node ownership: the update is silently skipped if the run's current
    ///     <c>NodeName</c> does not match the run's <c>NodeName</c> property, or if the run
    ///     is already in a terminal status.
    /// </summary>
    /// <param name="run">The run with updated fields. <c>NodeName</c> is used as a fencing token.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Atomically applies a run status transition using compare-and-swap. The transition succeeds only
    ///     if the run's current status and attempt match the expected values. Exactly one concurrent
    ///     caller wins. Timestamp fields supplied as null on <paramref name="transition" /> preserve the
    ///     existing stored values.
    ///     <para>
    ///         When a run with a non-null <c>BatchId</c> transitions to a terminal status, the store
    ///         atomically increments the batch progress counter and, if all children are now terminal,
    ///         completes the batch — all within the same transaction.
    ///     </para>
    /// </summary>
    /// <param name="transition">The transition request.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>
    ///     A <see cref="RunTransitionResult" /> indicating whether the transition was applied and,
    ///     if a batch was completed as a side-effect, the batch completion details.
    /// </returns>
    Task<RunTransitionResult> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Atomically cancels a run by transitioning it directly to <see cref="JobStatus.Cancelled" />.
    ///     Any non-terminal run (Pending or Running) is cancelled in a single atomic operation.
    ///     When <paramref name="expectedAttempt" /> is provided, the cancellation only applies if the
    ///     run's attempt matches (executor scoping). When null, any non-terminal run is cancelled.
    ///     <para>
    ///         The operation atomically inserts status events, caller-provided events, and updates
    ///         batch counters — all in the same transaction. Returns <see cref="RunTransitionResult" />
    ///         with batch completion info if applicable.
    ///     </para>
    /// </summary>
    /// <param name="runId">The run ID to cancel.</param>
    /// <param name="expectedAttempt">When non-null, only cancel if the run's attempt matches.</param>
    /// <param name="reason">Optional termination reason to set on the run.</param>
    /// <param name="events">Optional events to append atomically with the cancellation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>A <see cref="RunTransitionResult" /> indicating whether the run was cancelled and batch completion info.</returns>
    Task<RunTransitionResult> TryCancelRunAsync(string runId,
        int? expectedAttempt = null,
        string? reason = null,
        IReadOnlyList<RunEvent>? events = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Cancels all non-terminal runs belonging to the specified parent run.
    ///     Atomically inserts events and updates batch counters.
    /// </summary>
    /// <param name="parentRunId">The parent run ID whose children should be cancelled.</param>
    /// <param name="reason">Optional termination reason to set on cancelled runs.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The IDs of child runs that were cancelled.</returns>
    Task<IReadOnlyList<string>> CancelChildRunsAsync(string parentRunId,
        string? reason = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Atomically finds and claims up to <paramref name="maxCount" /> highest-priority pending runs
    ///     that match the node's registered jobs and queues. The claim operation verifies max concurrency
    ///     (job and queue), rate limits, and queue pause status in a single atomic operation. Each
    ///     claimed run's attempt is incremented as part of the claim.
    /// </summary>
    /// <param name="nodeName">The name of the claiming node.</param>
    /// <param name="jobNames">The job names this node is registered to process.</param>
    /// <param name="queueNames">The queue names this node is registered to process.</param>
    /// <param name="maxCount">The maximum number of runs to claim in this call. Must be at least 1.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The claimed runs with updated status, node, and attempt; empty when no eligible run was found.</returns>
    Task<IReadOnlyList<JobRun>> ClaimRunsAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, int maxCount, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Inserts a batch record and its child runs atomically. The batch starts with
    ///     <see cref="JobStatus.Pending" /> status and transitions when runs complete.
    /// </summary>
    /// <param name="batch">The batch record to create.</param>
    /// <param name="runs">The child runs belonging to the batch.</param>
    /// <param name="initialEvents">Optional events to append atomically with batch and run creation.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the batch record with the specified ID, or null if not found.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching batch record, or null.</returns>
    Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Atomically transitions a batch from a non-terminal status to the specified terminal status.
    ///     Returns true if the transition was applied; false if already terminal.
    ///     Used only by batch recovery (maintenance); the hot path updates batch status atomically
    ///     inside <see cref="TryTransitionRunAsync" /> and <see cref="TryCancelRunAsync" />.
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
    ///     Atomically inserts events and updates batch counters.
    /// </summary>
    /// <param name="batchId">The batch ID whose runs should be cancelled.</param>
    /// <param name="reason">Optional termination reason to set on cancelled runs.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The IDs of runs that were cancelled.</returns>
    Task<IReadOnlyList<string>> CancelBatchRunsAsync(string batchId,
        string? reason = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the direct children of a run (those whose <c>ParentRunId</c> equals
    ///     <paramref name="parentRunId" />), paginated by <c>(CreatedAt, Id)</c> keyset.
    ///     Use for tree-aware navigation where offset-based pagination over large parents
    ///     would scan too many rows.
    ///     <para>
    ///         Direction is determined by which cursor (if any) is supplied:
    ///         <list type="bullet">
    ///             <item>
    ///                 <description>Neither cursor: first page, ordered <c>(CreatedAt ASC, Id ASC)</c>.</description>
    ///             </item>
    ///             <item>
    ///                 <description>
    ///                     <paramref name="afterCursor" />: next forward page — items strictly
    ///                     greater than the cursor, ordered ASC.
    ///                 </description>
    ///             </item>
    ///             <item>
    ///                 <description>
    ///                     <paramref name="beforeCursor" />: reverse page — items strictly less than
    ///                     the cursor, returned in DESC order (most-recent-first). Callers reverse
    ///                     for chronological display.
    ///                 </description>
    ///             </item>
    ///         </list>
    ///     </para>
    /// </summary>
    /// <param name="parentRunId">The parent run ID.</param>
    /// <param name="afterCursor">
    ///     Opaque cursor from a previous response's <see cref="DirectChildrenPage.NextCursor" />.
    ///     Mutually exclusive with <paramref name="beforeCursor" />.
    /// </param>
    /// <param name="beforeCursor">
    ///     Reverse-direction cursor: returns items strictly less than this cursor in
    ///     DESC keyset order, capped at <paramref name="take" />.
    ///     Mutually exclusive with <paramref name="afterCursor" />.
    /// </param>
    /// <param name="take">Maximum number of children to return. Must be greater than 0.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The children page.</returns>
    Task<DirectChildrenPage> GetDirectChildrenAsync(string parentRunId,
        string? afterCursor = null,
        string? beforeCursor = null,
        int take = 50,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Walks the parent chain of <paramref name="runId" /> upward to the root, returning
    ///     the ancestors in root → immediate-parent order. The run itself is excluded. Returns
    ///     an empty list if the run is a root (no parent). Bounded by tree depth (typically small).
    /// </summary>
    /// <param name="runId">The run whose ancestors are requested.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>Ancestors in root → parent order; empty if the run has no parent.</returns>
    Task<IReadOnlyList<JobRun>> GetAncestorChainAsync(string runId,
        CancellationToken cancellationToken = default);

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
    ///     Returns events across all direct child runs of a batch, ordered by event ID ascending
    ///     for global cross-child event ordering.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="sinceEventId">Only return events with an ID greater than this value.</param>
    /// <param name="take">The maximum number of events to return.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching events ordered by ID ascending.</returns>
    Task<IReadOnlyList<RunEvent>> GetBatchEventsAsync(string batchId, long sinceEventId = 0, int take = 200,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns output events across all direct child runs of a batch, ordered by
    ///     event ID ascending for global cross-child event ordering.
    /// </summary>
    /// <param name="batchId">The batch ID.</param>
    /// <param name="sinceEventId">Only return events with an ID greater than this value.</param>
    /// <param name="take">The maximum number of events to return.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching output events ordered by ID ascending.</returns>
    Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchId, long sinceEventId = 0, int take = 200,
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
    ///     Returns the subset of <paramref name="runIds" /> that are no longer in
    ///     <see cref="JobStatus.Running" /> status: either deleted or in a terminal/non-running status.
    ///     Used by the executor to react to externally-initiated cancellation when notifications
    ///     are missed.
    /// </summary>
    /// <param name="runIds">The IDs of runs currently executing on this node.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The IDs from the input set that should be stopped.</returns>
    Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(IReadOnlyCollection<string> runIds,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns up to <paramref name="take" /> IDs of runs in <see cref="JobStatus.Running" /> whose
    ///     <c>LastHeartbeatAt</c> is strictly less than <paramref name="staleBefore" />, ordered oldest first.
    ///     Dedicated maintenance query: returns IDs only (no row materialization) and never scans the full
    ///     table or computes totals. Implementations must be backed by an index that covers the
    ///     <c>(status = Running, LastHeartbeatAt)</c> predicate so the query cost is bounded by the
    ///     result size. Callers drain stale runs by calling this in a loop until it returns fewer than
    ///     <paramref name="take" /> rows (each processed run transitions out of Running, leaving the filter set).
    /// </summary>
    /// <param name="staleBefore">Heartbeat cutoff; runs with <c>LastHeartbeatAt &lt; staleBefore</c> are returned.</param>
    /// <param name="take">Maximum number of IDs to return. Must be greater than 0.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>IDs of stale running runs ordered oldest heartbeat first.</returns>
    Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
        CancellationToken cancellationToken = default);

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
    Task<NodeInfo?> GetNodeAsync(string name, CancellationToken cancellationToken = default);

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
    ///     Rate limit acquisition is embedded inside <see cref="ClaimRunsAsync" /> -- there is no
    ///     standalone acquire method.
    /// </summary>
    /// <param name="rateLimit">The rate limit definition to upsert.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Cancels all pending runs whose <c>NotAfter</c> deadline has passed and
    ///     returns the run IDs that were transitioned to <see cref="JobStatus.Cancelled" />.
    ///     Atomically updates batch counters for cancelled batch children.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The IDs of runs that were cancelled in this operation.</returns>
    Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(CancellationToken cancellationToken = default);

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

    /// <summary>
    ///     Returns the IDs of non-terminal batches where all runs have completed
    ///     but the batch has not yet been marked terminal. Used for stuck-batch recovery.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>IDs of batches that can be completed.</returns>
    Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns whether the given exception represents a transient store failure that is
    ///     safe to retry (e.g. deadlock, connection reset, lock timeout). Non-transient
    ///     exceptions (constraint violations, schema errors, auth failures) return false.
    /// </summary>
    bool IsTransientException(Exception ex) => false;
}