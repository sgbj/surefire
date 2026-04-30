using System.Diagnostics.CodeAnalysis;
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

    internal static DashboardStatsResponse From(DashboardStats stats, IReadOnlyList<JobRun> recentRuns) => new()
    {
        TotalJobs = stats.TotalJobs,
        TotalRuns = stats.TotalRuns,
        ActiveRuns = stats.ActiveRuns,
        SuccessRate = stats.SuccessRate * 100,
        NodeCount = stats.NodeCount,
        RunsByStatus = stats.RunsByStatus,
        Timeline = stats.Timeline.Select(TimelineBucketResponse.From).ToList(),
        RecentRuns = recentRuns.Select(r => RunResponse.From(r)).ToList()
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
    ///     Optional depth in a trace hierarchy. Populated by the trace endpoint so the
    ///     client can render a tree without rebuilding it. Omitted from non-trace
    ///     responses; the UI treats presence as "this run came from a trace view".
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

    /// <summary>Current attempt number; starts at 1 and increments on each retry.</summary>
    public int Attempt { get; init; }

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

    /// <summary>Run priority. Higher values are claimed first.</summary>
    public int Priority { get; init; }

    /// <summary>Optional deduplication id; only one non-terminal run per <c>(JobName, DeduplicationId)</c> may exist.</summary>
    public string? DeduplicationId { get; init; }

    /// <summary>Last heartbeat time recorded by the executing node.</summary>
    public DateTimeOffset? LastHeartbeatAt { get; init; }

    /// <summary>Identifier of the batch this run belongs to, or null if the run was triggered standalone.</summary>
    public string? BatchId { get; init; }

    internal static RunResponse From(JobRun run, int? depth = null) => new()
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
        TraceId = run.TraceId,
        SpanId = run.SpanId,
        ParentTraceId = run.ParentTraceId,
        ParentSpanId = run.ParentSpanId,
        ParentRunId = run.ParentRunId,
        RootRunId = run.RootRunId,
        RerunOfRunId = run.RerunOfRunId,
        NotBefore = run.NotBefore,
        NotAfter = run.NotAfter,
        Priority = run.Priority,
        DeduplicationId = run.DeduplicationId,
        LastHeartbeatAt = run.LastHeartbeatAt,
        BatchId = run.BatchId,
        Depth = depth
    };
}

/// <summary>
///     Focused trace response: ancestor chain (root to immediate parent), the run itself,
///     a window of siblings around it, and the first page of direct children. Enough
///     context for the client to render a tree view without rebuilding it from a flat list.
/// </summary>
public sealed class RunTraceResponse
{
    /// <summary>Ancestors from root to immediate parent, each with a <see cref="RunResponse.Depth" /> starting at 0.</summary>
    public required IReadOnlyList<RunResponse> Ancestors { get; init; }

    /// <summary>The focus run itself. Depth equals the ancestors count.</summary>
    public required RunResponse Focus { get; init; }

    /// <summary>Siblings ordered before the focus (in parent-order). Empty if focus has no parent.</summary>
    public required IReadOnlyList<RunResponse> SiblingsBefore { get; init; }

    /// <summary>Siblings ordered after the focus (in parent-order). Empty if focus has no parent.</summary>
    public required IReadOnlyList<RunResponse> SiblingsAfter { get; init; }

    /// <summary>Cursor for loading more siblings before/after.</summary>
    public SiblingsCursorResponse? SiblingsCursor { get; init; }

    /// <summary>First page of direct children of the focus.</summary>
    public required IReadOnlyList<RunResponse> Children { get; init; }

    /// <summary>Cursor for loading more children, or null if all children are included.</summary>
    public string? ChildrenCursor { get; init; }
}

/// <summary>Bidirectional cursors for paging the siblings window in a trace response.</summary>
public sealed class SiblingsCursorResponse
{
    /// <summary>Cursor to fetch more siblings ordered after the current <c>SiblingsAfter</c>; null if no more.</summary>
    public string? After { get; init; }

    /// <summary>Cursor to fetch more siblings ordered before the current <c>SiblingsBefore</c>; null if no more.</summary>
    public string? Before { get; init; }
}

/// <summary>Response for the direct-children pagination endpoint.</summary>
public sealed class RunChildrenResponse
{
    /// <summary>The page of direct children, ordered by <c>(CreatedAt, Id)</c> ascending.</summary>
    public required IReadOnlyList<RunResponse> Items { get; init; }

    /// <summary>Cursor for the next forward page, or null if this is the last page.</summary>
    public string? NextCursor { get; init; }
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
    [RequiresUnreferencedCode("Deserializes the arguments schema as a JsonElement.")]
    [RequiresDynamicCode("Deserializes the arguments schema as a JsonElement.")]
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
        ArgumentsSchema = job.ArgumentsSchema is { }
            ? JsonSerializer.Deserialize<JsonElement>(job.ArgumentsSchema)
            : null
    };

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
