namespace Surefire;

public interface IJobStore
{
    Task MigrateAsync(CancellationToken cancellationToken = default);

    Task UpsertJobAsync(JobDefinition job, CancellationToken cancellationToken = default);
    Task<JobDefinition?> GetJobAsync(string name, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null, CancellationToken cancellationToken = default);

    Task CreateRunAsync(JobRun run, CancellationToken cancellationToken = default);
    Task CreateRunsAsync(IReadOnlyList<JobRun> runs, CancellationToken cancellationToken = default);
    Task<bool> TryCreateRunAsync(JobRun run, CancellationToken cancellationToken = default);
    Task<bool> TryCreateContinuousRunAsync(JobRun run, CancellationToken cancellationToken = default);
    Task<JobRun?> GetRunAsync(string id, CancellationToken cancellationToken = default);
    Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default);
    Task UpdateRunAsync(JobRun run, CancellationToken cancellationToken = default);

    /// <summary>
    /// Atomically transitions a run from <paramref name="expectedStatus"/> to the status in <paramref name="run"/>.
    /// Returns true if the update was applied, false if the run's current status did not match.
    /// Used by recovery paths to prevent double-recovery race conditions.
    /// </summary>
    Task<bool> TryUpdateRunStatusAsync(JobRun run, JobStatus expectedStatus, CancellationToken cancellationToken = default);
    Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, IReadOnlyCollection<string> queueNames, CancellationToken cancellationToken = default);

    Task RemoveJobAsync(string name, CancellationToken cancellationToken = default);
    Task SetJobEnabledAsync(string name, bool enabled, CancellationToken cancellationToken = default);

    Task RegisterNodeAsync(NodeInfo node, CancellationToken cancellationToken = default);
    Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> registeredJobNames, IReadOnlyCollection<string> registeredQueueNames, CancellationToken cancellationToken = default);
    Task RemoveNodeAsync(string nodeName, CancellationToken cancellationToken = default);
    Task<NodeInfo?> GetNodeAsync(string nodeName, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken cancellationToken = default);

    Task AppendEventAsync(RunEvent evt, CancellationToken cancellationToken = default);
    Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0, RunEventType[]? types = null, CancellationToken cancellationToken = default);

    Task HeartbeatRunsAsync(IReadOnlyCollection<string> runIds, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<JobRun>> GetInactiveRunsAsync(TimeSpan threshold, CancellationToken cancellationToken = default);

    Task<IReadOnlyList<JobRun>> GetRunTraceAsync(string runId, int limit = 200, CancellationToken cancellationToken = default);

    Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60, CancellationToken cancellationToken = default);
    Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken cancellationToken = default);
    Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(CancellationToken cancellationToken = default);

    Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken cancellationToken = default);
    Task<bool> TryAcquireRateLimitAsync(string name, CancellationToken cancellationToken = default);

    Task UpsertQueueAsync(QueueDefinition queue, CancellationToken cancellationToken = default);
    Task<QueueDefinition?> GetQueueAsync(string name, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken cancellationToken = default);
    Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels pending runs whose NotAfter deadline has passed.
    /// Returns the number of runs cancelled.
    /// </summary>
    Task<int> CancelExpiredRunsAsync(CancellationToken cancellationToken = default);

    Task<int> PurgeRunsAsync(DateTimeOffset completedBefore, CancellationToken cancellationToken = default);
    Task<int> PurgeJobsAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default);
    Task<int> PurgeQueuesAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default);
    Task<int> PurgeNodesAsync(DateTimeOffset heartbeatBefore, CancellationToken cancellationToken = default);
}
