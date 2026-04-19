namespace Surefire.Tests.Fakes;

/// <summary>
///     Base <see cref="IJobStore" /> implementation where every method throws
///     <see cref="NotImplementedException" />. Test fakes extend this and override
///     only the methods they exercise.
/// </summary>
internal abstract class ThrowingJobStore : IJobStore
{
    public virtual Task MigrateAsync(CancellationToken ct = default) => throw new NotImplementedException();
    public virtual Task PingAsync(CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task UpsertJobAsync(JobDefinition job, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<JobDefinition?> GetJobAsync(string name, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task SetJobEnabledAsync(string name, bool enabled, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task
        UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task CreateRunsAsync(IReadOnlyList<JobRun> runs, IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
        DateTimeOffset? lastCronFireAt = null, IReadOnlyList<RunEvent>? initialEvents = null,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task<JobRun?> GetRunAsync(string id, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyList<JobRun>> GetRunsByIdsAsync(IReadOnlyList<string> ids,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, int skip = 0, int take = 50,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task UpdateRunAsync(JobRun run, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<RunTransitionResult> TryTransitionRunAsync(RunStatusTransition transition,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task<RunTransitionResult> TryCancelRunAsync(string runId, int? expectedAttempt = null,
        string? reason = null, IReadOnlyList<RunEvent>? events = null, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyList<string>> CancelChildRunsAsync(string parentRunId, string? reason = null,
        CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public virtual Task<IReadOnlyList<string>> CancelBatchRunsAsync(string batchId, string? reason = null,
        CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public virtual Task<IReadOnlyList<JobRun>> ClaimRunsAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, int maxCount, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs,
        IReadOnlyList<RunEvent>? initialEvents = null, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<bool> TryCompleteBatchAsync(string batchId, JobStatus status, DateTimeOffset completedAt,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0,
        RunEventType[]? types = null, int? attempt = null, int? take = null, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyList<RunEvent>> GetBatchEventsAsync(string batchId, long sinceEventId = 0,
        int take = 200, CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchId, long sinceEventId = 0,
        int take = 200, CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames,
        IReadOnlyCollection<string> queueNames, IReadOnlyCollection<string> activeRunIds,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task<IReadOnlyList<string>> GetExternallyStoppedRunIdsAsync(IReadOnlyCollection<string> runIds,
        CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public virtual Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
        CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public virtual Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<NodeInfo?> GetNodeAsync(string name, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public virtual Task UpsertQueueAsync(QueueDefinition queue, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyList<string>> CancelExpiredRunsWithIdsAsync(
        CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public virtual Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(
        CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public virtual Task PurgeAsync(DateTimeOffset threshold, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60,
        CancellationToken ct = default) => throw new NotImplementedException();

    public virtual Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<DirectChildrenPage> GetDirectChildrenAsync(string parentRunId, string? afterCursor = null,
        string? beforeCursor = null, int take = 50, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual Task<IReadOnlyList<JobRun>> GetAncestorChainAsync(string runId, CancellationToken ct = default) =>
        throw new NotImplementedException();

    public virtual bool IsTransientException(Exception ex) => false;
}