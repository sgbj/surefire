using System.Text.Json;
using System.Text.Json.Serialization;

namespace Surefire.Dashboard;

/// <summary>
///     Aggregate dashboard statistics: status counts, success rate, node count, a timeline of
///     runs bucketed by status, and a tail of recent runs for quick context.
/// </summary>
public sealed class DashboardStatsResponse
{
    /// <summary>Total number of registered jobs across all nodes.</summary>
    public int TotalJobs { get; init; }

    /// <summary>Total number of runs in the store, across all statuses.</summary>
    public int TotalRuns { get; init; }

    /// <summary>Number of non-terminal runs (Pending or Running).</summary>
    public int ActiveRuns { get; init; }

    /// <summary>Percentage of terminal runs that succeeded, in the range <c>[0, 100]</c>.</summary>
    public double SuccessRate { get; init; }

    /// <summary>Number of nodes that have heartbeated within the active threshold.</summary>
    public int NodeCount { get; init; }

    /// <summary>Run counts keyed by <see cref="JobStatus" /> name.</summary>
    public IReadOnlyDictionary<string, int> RunsByStatus { get; init; } = new Dictionary<string, int>();

    /// <summary>Time-bucketed run counts for the dashboard chart.</summary>
    public IReadOnlyList<TimelineBucketResponse> Timeline { get; init; } = [];

    /// <summary>Most recent runs across all jobs, newest first.</summary>
    public IReadOnlyList<RunResponse> RecentRuns { get; init; } = [];

    internal static DashboardStatsResponse From(DashboardStats stats, IReadOnlyList<RunResponse> recentRuns) => new()
    {
        TotalJobs = stats.TotalJobs,
        TotalRuns = stats.TotalRuns,
        ActiveRuns = stats.ActiveRuns,
        SuccessRate = stats.SuccessRate * 100,
        NodeCount = stats.NodeCount,
        RunsByStatus = stats.RunsByStatus,
        Timeline = stats.Timeline.Select(TimelineBucketResponse.From).ToList(),
        RecentRuns = recentRuns
    };
}

/// <summary>JSON shape for a <see cref="JobRun" /> returned by the dashboard API.</summary>
public sealed class RunResponse
{
    /// <summary>The run identifier.</summary>
    public required string Id { get; init; }

    /// <summary>The registered job name this run executes.</summary>
    public required string JobName { get; init; }

    /// <summary>Current run status.</summary>
    public JobStatus Status { get; init; }

    /// <summary>Serialized arguments JSON, or null when the run was triggered without arguments.</summary>
    public string? Arguments { get; init; }

    /// <summary>Serialized terminal result JSON, or null when no result was produced.</summary>
    public string? Result { get; init; }

    /// <summary>Termination reason for non-success terminals, or null otherwise.</summary>
    public string? Reason { get; init; }

    /// <summary>
    ///     Optional depth in a trace hierarchy. Populated by the tree endpoint so the
    ///     client can render the run tree without rebuilding it. Omitted from
    ///     non-tree responses; the UI treats presence as "this run came from a tree view".
    /// </summary>
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Depth { get; init; }

    /// <summary>Last reported progress in the range <c>[0, 1]</c>.</summary>
    public double Progress { get; init; }

    /// <summary>When the run was created.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>When the run started executing, or null if it has not started.</summary>
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>When the run reached a terminal status, or null if it is still active.</summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>When the run was canceled, or null if it was not canceled.</summary>
    public DateTimeOffset? CanceledAt { get; init; }

    /// <summary>Name of the node currently or most recently executing this run.</summary>
    public string? NodeName { get; init; }

    /// <summary>
    ///     Failure-aware attempt number; starts at 1 and increments on each real handler retry.
    ///     For durable orchestrators this excludes suspend/resume replay cycles
    ///     (use <see cref="ReplayCount" /> for those).
    /// </summary>
    public int Attempt { get; init; }

    /// <summary>Number of real handler failures recorded for this run.</summary>
    public int FailureCount { get; init; }

    /// <summary>
    ///     Number of suspend/resume replay cycles this run has been through. Always <c>0</c> for
    ///     non-durable jobs. For durable orchestrators awaiting N children, this can be up to N as
    ///     each child terminal triggers a replay.
    /// </summary>
    public int ReplayCount { get; init; }

    /// <summary>OpenTelemetry trace ID for this run, or null when tracing is unavailable.</summary>
    public string? TraceId { get; init; }

    /// <summary>OpenTelemetry span ID for this run.</summary>
    public string? SpanId { get; init; }

    /// <summary>Trace ID of the activity that triggered this run, propagated for cross-run correlation.</summary>
    public string? ParentTraceId { get; init; }

    /// <summary>Span ID of the activity that triggered this run.</summary>
    public string? ParentSpanId { get; init; }

    /// <summary>Identifier of the run that triggered this one, or null if this run is a root.</summary>
    public string? ParentRunId { get; init; }

    /// <summary>Identifier of the topmost ancestor run, or this run's id if it is a root.</summary>
    public string? RootRunId { get; init; }

    /// <summary>Identifier of the original run when this run is a rerun.</summary>
    public string? RerunOfRunId { get; init; }

    /// <summary>Earliest time the run is eligible to be claimed.</summary>
    public DateTimeOffset NotBefore { get; init; }

    /// <summary>Deadline after which a still-pending run is automatically canceled.</summary>
    public DateTimeOffset? NotAfter { get; init; }

    /// <summary>Deadline after which a non-terminal run is automatically canceled.</summary>
    public DateTimeOffset? ExpiresAt { get; init; }

    /// <summary>Run priority. Higher values are claimed first.</summary>
    public int Priority { get; init; }

    /// <summary>Optional deduplication id; only one non-terminal run per <c>(JobName, DeduplicationId)</c> may exist.</summary>
    public string? DeduplicationId { get; init; }

    /// <summary>Last heartbeat time recorded by the executing node.</summary>
    public DateTimeOffset? LastHeartbeatAt { get; init; }

    /// <summary>Identifier of the batch this run belongs to, or null if the run was triggered standalone.</summary>
    public string? BatchId { get; init; }

    internal static RunResponse From(JobRun run, int? depth = null)
    {
        return new()
        {
            Id = run.Id,
            JobName = run.JobName,
            Status = run.Status,
            Arguments = run.Arguments,
            Result = run.Result,
            Reason = run.Reason,
            Progress = run.Progress,
            CreatedAt = run.CreatedAt,
            StartedAt = run.StartedAt,
            CompletedAt = run.CompletedAt,
            CanceledAt = run.CanceledAt,
            NodeName = run.NodeName,
            Attempt = run.Attempt,
            FailureCount = run.FailureCount,
            ReplayCount = run.ReplayCount,
            TraceId = run.TraceId,
            SpanId = run.SpanId,
            ParentTraceId = run.ParentTraceId,
            ParentSpanId = run.ParentSpanId,
            ParentRunId = run.ParentRunId,
            RootRunId = run.RootRunId,
            RerunOfRunId = run.RerunOfRunId,
            NotBefore = run.NotBefore,
            NotAfter = run.NotAfter,
            ExpiresAt = run.ExpiresAt,
            Priority = run.Priority,
            DeduplicationId = run.DeduplicationId,
            LastHeartbeatAt = run.LastHeartbeatAt,
            BatchId = run.BatchId,
            Depth = depth
        };
    }
}

/// <summary>Request body for bulk refreshing run rows by ID.</summary>
public sealed class RunLookupRequest
{
    /// <summary>Run IDs to fetch. Duplicates and blank values are ignored.</summary>
    public IReadOnlyList<string> Ids { get; init; } = [];
}

/// <summary>
///     Complete run-tree response: every run in the focus's hierarchy (the root and every
///     descendant reachable through <c>ParentRunId</c>), in a single payload. Backed by the
///     <c>RootRunId</c> index, so the entire tree is one query. Each run carries
///     <see cref="RunResponse.Depth" /> and <see cref="RunResponse.ParentRunId" />; the client
///     can flatten in DFS order without any further round-trips. The flat list is sorted by
///     <c>(CreatedAt, Id)</c> ascending so siblings appear in stable chronological order.
/// </summary>
public sealed class RunTreeResponse
{
    /// <summary>The topmost ancestor's id. Always present; equals the focus id when the focus is a root.</summary>
    public required string RootId { get; init; }

    /// <summary>Every run in the tree, sorted by <c>(CreatedAt, Id)</c> ascending, with <c>Depth</c> populated.</summary>
    public required IReadOnlyList<RunResponse> Runs { get; init; }

    /// <summary>
    ///     True when the tree exceeded the server cap and <see cref="Runs" /> is a partial result.
    ///     <see cref="TotalCount" /> is authoritative either way.
    /// </summary>
    public required bool Truncated { get; init; }

    /// <summary>Total number of runs in the tree, including any not returned when <see cref="Truncated" /> is true.</summary>
    public required int TotalCount { get; init; }
}

/// <summary>Generic offset-paginated response with a total count.</summary>
/// <typeparam name="T">The item shape.</typeparam>
public sealed class PagedResponse<T>
{
    /// <summary>The page of items.</summary>
    public required IReadOnlyList<T> Items { get; init; }

    /// <summary>Total number of items matching the query, across all pages.</summary>
    public int TotalCount { get; init; }

    /// <summary>Maps a <see cref="PagedResult{T}" /> to a <see cref="PagedResponse{T}" /> via <paramref name="map" />.</summary>
    public static PagedResponse<TOut> From<TIn, TOut>(PagedResult<TIn> result, Func<TIn, TOut> map) => new()
    {
        Items = result.Items.Select(map).ToList(),
        TotalCount = result.TotalCount
    };
}

/// <summary>Per-job statistics: counts, success rate, average duration, and last run time.</summary>
public sealed class JobStatsResponse
{
    /// <summary>Total runs of this job, across all statuses.</summary>
    public int TotalRuns { get; init; }

    /// <summary>Number of runs that succeeded.</summary>
    public int SucceededRuns { get; init; }

    /// <summary>Number of runs that exhausted retries and failed.</summary>
    public int FailedRuns { get; init; }

    /// <summary>Percentage of terminal runs that succeeded, in the range <c>[0, 100]</c>.</summary>
    public double SuccessRate { get; init; }

    /// <summary>Average duration of completed runs, or null if no runs have completed.</summary>
    public TimeSpan? AvgDuration { get; init; }

    /// <summary>Timestamp of the most recent run, or null if the job has never run.</summary>
    public DateTimeOffset? LastRunAt { get; init; }

    /// <summary>Maps a <see cref="JobStats" /> to its dashboard response shape.</summary>
    public static JobStatsResponse From(JobStats stats) => new()
    {
        TotalRuns = stats.TotalRuns,
        SucceededRuns = stats.SucceededRuns,
        FailedRuns = stats.FailedRuns,
        SuccessRate = stats.SuccessRate * 100,
        AvgDuration = stats.AvgDuration,
        LastRunAt = stats.LastRunAt
    };
}

/// <summary>One bucket in the dashboard timeline: status counts within a fixed time window.</summary>
public sealed class TimelineBucketResponse
{
    /// <summary>Start of the bucket window.</summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>Pending runs in this bucket.</summary>
    public int Pending { get; init; }

    /// <summary>Running runs in this bucket.</summary>
    public int Running { get; init; }

    /// <summary>Suspended durable orchestrator runs in this bucket.</summary>
    public int Suspended { get; init; }

    /// <summary>Successfully completed runs in this bucket.</summary>
    public int Succeeded { get; init; }

    /// <summary>Canceled runs in this bucket.</summary>
    public int Canceled { get; init; }

    /// <summary>Failed runs in this bucket.</summary>
    public int Failed { get; init; }

    /// <summary>Maps a <see cref="TimelineBucket" /> to its dashboard response shape.</summary>
    public static TimelineBucketResponse From(TimelineBucket bucket) => new()
    {
        Timestamp = bucket.Start,
        Pending = bucket.Pending,
        Running = bucket.Running,
        Suspended = bucket.Suspended,
        Succeeded = bucket.Succeeded,
        Canceled = bucket.Canceled,
        Failed = bucket.Failed
    };
}

/// <summary>JSON shape for a <see cref="JobDefinition" /> returned by the dashboard API.</summary>
public sealed class JobResponse
{
    /// <summary>The job name.</summary>
    public required string Name { get; init; }

    /// <summary>Optional human-readable description.</summary>
    public string? Description { get; init; }

    /// <summary>Tags applied to the job.</summary>
    public string[] Tags { get; init; } = [];

    /// <summary>Cron expression for scheduled execution, or null if the job is not on a schedule.</summary>
    public string? CronExpression { get; init; }

    /// <summary>Time zone the cron expression evaluates against. Null evaluates as UTC.</summary>
    public string? TimeZoneId { get; init; }

    /// <summary>Hard timeout per execution attempt, or null for no per-attempt timeout.</summary>
    public TimeSpan? Timeout { get; init; }

    /// <summary>Maximum concurrent runs of this job across all nodes, or null for unlimited.</summary>
    public int? MaxConcurrency { get; init; }

    /// <summary>Run priority. Higher values are claimed first.</summary>
    public int Priority { get; init; }

    /// <summary>Retry policy applied to failed attempts.</summary>
    public RetryPolicy RetryPolicy { get; init; } = new();

    /// <summary>Whether this is a continuous job (auto-restarts after each terminal run).</summary>
    public bool IsContinuous { get; init; }

    /// <summary>Whether the job is currently enabled for scheduling and triggering.</summary>
    public bool IsEnabled { get; init; }

    /// <summary>Queue this job runs on, or null when the job uses the default queue.</summary>
    public string? Queue { get; init; }

    /// <summary>Name of the rate limit applied to this job, or null when unlimited.</summary>
    public string? RateLimitName { get; init; }

    /// <summary>
    ///     Last time a node confirmed it can serve this job; older than the active threshold means no live node serves
    ///     it.
    /// </summary>
    public DateTimeOffset? LastHeartbeatAt { get; init; }

    /// <summary>Whether at least one live node is currently registered to serve this job.</summary>
    public bool IsActive { get; init; }

    /// <summary>Next computed cron occurrence, or null when the job is disabled or has no cron.</summary>
    public DateTimeOffset? NextRunAt { get; init; }

    /// <summary>Misfire policy applied when the scheduler is behind on cron occurrences.</summary>
    public MisfirePolicy MisfirePolicy { get; init; }

    /// <summary>JSON Schema describing the job's argument shape, or null if the handler takes no bindable arguments.</summary>
    public JsonElement? ArgumentsSchema { get; init; }

    /// <summary>Maps a <see cref="JobDefinition" /> to its dashboard response shape.</summary>
    /// <param name="job">The job definition.</param>
    /// <param name="activeCutoff">Heartbeat cutoff used to compute <see cref="IsActive" />.</param>
    /// <param name="now">Current time used to compute <see cref="NextRunAt" />.</param>
    public static JobResponse From(JobDefinition job, DateTimeOffset activeCutoff, DateTimeOffset now) => new()
    {
        Name = job.Name,
        Description = job.Description,
        Tags = job.Tags,
        CronExpression = job.CronExpression,
        TimeZoneId = job.TimeZoneId,
        Timeout = job.Timeout,
        MaxConcurrency = job.MaxConcurrency,
        Priority = job.Priority,
        RetryPolicy = job.RetryPolicy,
        IsContinuous = job.IsContinuous,
        IsEnabled = job.IsEnabled,
        Queue = job.Queue,
        RateLimitName = job.RateLimitName,
        LastHeartbeatAt = job.LastHeartbeatAt,
        IsActive = job.LastHeartbeatAt is { } && job.LastHeartbeatAt >= activeCutoff,
        NextRunAt = ComputeNextRun(job, now),
        MisfirePolicy = job.MisfirePolicy,
        ArgumentsSchema = CloneElement(job.ArgumentsSchema)
    };

    // JsonDocument.Parse is AOT-safe (it walks JSON tokens rather than reflecting over a target
    // type) but holds pooled buffers - dispose the document and keep just the cloned element.
    private static JsonElement? CloneElement(string? json)
    {
        if (json is null)
        {
            return null;
        }

        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.Clone();
    }

    private static DateTimeOffset? ComputeNextRun(JobDefinition job, DateTimeOffset now)
    {
        if (job.CronExpression is null || !job.IsEnabled)
        {
            return null;
        }

        try
        {
            if (!CronScheduleValidation.TryParseCron(job.CronExpression, out var cron) ||
                !CronScheduleValidation.TryResolveTimeZone(job.TimeZoneId, out var timeZone))
            {
                return null;
            }

            var next = cron.GetNextOccurrence(now.UtcDateTime, timeZone);
            return next.HasValue ? new DateTimeOffset(next.Value, TimeSpan.Zero) : null;
        }
        catch
        {
            return null;
        }
    }
}

/// <summary>JSON shape for a <see cref="NodeInfo" /> returned by the dashboard API.</summary>
public sealed class NodeResponse
{
    /// <summary>Unique node name.</summary>
    public required string Name { get; init; }

    /// <summary>When the node first registered with the cluster.</summary>
    public DateTimeOffset StartedAt { get; init; }

    /// <summary>Last heartbeat time recorded for this node.</summary>
    public DateTimeOffset LastHeartbeatAt { get; init; }

    /// <summary>Number of runs currently executing on this node.</summary>
    public int RunningCount { get; init; }

    /// <summary>Job names this node is registered to serve.</summary>
    public IReadOnlyList<string> RegisteredJobNames { get; init; } = [];

    /// <summary>Whether this node has heartbeated within the active threshold.</summary>
    public bool IsActive { get; init; }

    /// <summary>Queue names this node is registered to serve.</summary>
    public IReadOnlyList<string> RegisteredQueueNames { get; init; } = [];

    /// <summary>Maps a <see cref="NodeInfo" /> to its dashboard response shape.</summary>
    /// <param name="node">The node record.</param>
    /// <param name="activeCutoff">Heartbeat cutoff used to compute <see cref="IsActive" />.</param>
    public static NodeResponse From(NodeInfo node, DateTimeOffset activeCutoff) => new()
    {
        Name = node.Name,
        StartedAt = node.StartedAt,
        LastHeartbeatAt = node.LastHeartbeatAt,
        RunningCount = node.RunningCount,
        RegisteredJobNames = node.RegisteredJobNames,
        RegisteredQueueNames = node.RegisteredQueueNames,
        IsActive = node.LastHeartbeatAt >= activeCutoff
    };
}

/// <summary>Queue settings and live counts surfaced to the dashboard.</summary>
public sealed class QueueResponse
{
    /// <summary>Unique queue name.</summary>
    public required string Name { get; init; }

    /// <summary>Queue priority. Higher-priority queues are drained first when nodes have capacity.</summary>
    public int Priority { get; init; }

    /// <summary>Maximum concurrent runs across all nodes, or null for unlimited.</summary>
    public int? MaxConcurrency { get; init; }

    /// <summary>Whether the queue is paused; paused queues do not have runs claimed from them.</summary>
    public bool IsPaused { get; init; }

    /// <summary>Name of the rate limit applied to this queue, or null when unlimited.</summary>
    public string? RateLimitName { get; init; }

    /// <summary>Number of pending runs currently waiting in this queue.</summary>
    public int PendingCount { get; init; }

    /// <summary>Number of runs currently executing from this queue.</summary>
    public int RunningCount { get; init; }

    /// <summary>Names of nodes registered to serve this queue.</summary>
    public IReadOnlyList<string> ProcessingNodes { get; init; } = [];
}

/// <summary>Body for the trigger-job endpoint. All fields are optional.</summary>
public sealed class TriggerJobRequest
{
    /// <summary>Arguments JSON, bound to the handler's parameters by name.</summary>
    public JsonElement? Args { get; set; }

    /// <summary>Earliest time the run is eligible to be claimed.</summary>
    public DateTimeOffset? NotBefore { get; set; }

    /// <summary>Deadline after which a still-pending run is automatically canceled.</summary>
    public DateTimeOffset? NotAfter { get; set; }

    /// <summary>Deadline after which a non-terminal run is automatically canceled.</summary>
    public DateTimeOffset? ExpiresAt { get; set; }

    /// <summary>Run priority. Higher values are claimed first.</summary>
    public int? Priority { get; set; }

    /// <summary>Optional deduplication id; only one non-terminal run per <c>(JobName, DeduplicationId)</c> may exist.</summary>
    public string? DeduplicationId { get; set; }
}

/// <summary>Body for the update-job endpoint. Null fields are left unchanged.</summary>
public sealed class UpdateJobRequest
{
    /// <summary>When set, enables or disables the job.</summary>
    public bool? IsEnabled { get; set; }
}

/// <summary>Body for the update-queue endpoint. Null fields are left unchanged.</summary>
public sealed class UpdateQueueRequest
{
    /// <summary>When set, pauses or unpauses the queue.</summary>
    public bool? IsPaused { get; set; }
}

/// <summary>Cursor-paginated page of run log events.</summary>
public sealed class LogPageResponse
{
    /// <summary>The page of log events as raw JSON elements.</summary>
    public required IReadOnlyList<JsonElement> Items { get; init; }

    /// <summary>Cursor for the next forward page, or null if this is the last page.</summary>
    public long? NextCursor { get; init; }
}

/// <summary>Response containing a single run identifier (returned by trigger and rerun endpoints).</summary>
/// <param name="RunId">The created run identifier.</param>
public sealed record RunIdResponse(string RunId);
