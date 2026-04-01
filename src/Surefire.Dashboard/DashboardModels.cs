using System.Text.Json;

namespace Surefire.Dashboard;

// Response models

public sealed class DashboardStatsResponse
{
    public int TotalJobs { get; init; }
    public int TotalRuns { get; init; }
    public int ActiveRuns { get; init; }
    public double SuccessRate { get; init; }
    public int NodeCount { get; init; }
    public IReadOnlyDictionary<string, int> RunsByStatus { get; init; } = new Dictionary<string, int>();
    public IReadOnlyList<TimelineBucketResponse> Timeline { get; init; } = [];
    public IReadOnlyList<RunResponse> RecentRuns { get; init; } = [];

    public static DashboardStatsResponse From(DashboardStats stats, IReadOnlyList<JobRun> recentRuns) => new()
    {
        TotalJobs = stats.TotalJobs,
        TotalRuns = stats.TotalRuns,
        ActiveRuns = stats.ActiveRuns,
        SuccessRate = stats.SuccessRate,
        NodeCount = stats.NodeCount,
        RunsByStatus = stats.RunsByStatus,
        Timeline = stats.Timeline.Select(TimelineBucketResponse.From).ToList(),
        RecentRuns = recentRuns.Select(RunResponse.From).ToList()
    };
}

public sealed class RunResponse
{
    public required string Id { get; init; }
    public required string JobName { get; init; }
    public JobStatus Status { get; init; }
    public string? Arguments { get; init; }
    public string? Result { get; init; }
    public string? Error { get; init; }
    public double Progress { get; init; }
    public DateTimeOffset CreatedAt { get; init; }
    public DateTimeOffset? StartedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public DateTimeOffset? CancelledAt { get; init; }
    public string? NodeName { get; init; }
    public int Attempt { get; init; }
    public string? TraceId { get; init; }
    public string? SpanId { get; init; }
    public string? ParentRunId { get; init; }
    public string? RootRunId { get; init; }
    public string? RerunOfRunId { get; init; }
    public DateTimeOffset NotBefore { get; init; }
    public DateTimeOffset? NotAfter { get; init; }
    public int Priority { get; init; }
    public int QueuePriority { get; init; }
    public string? DeduplicationId { get; init; }
    public DateTimeOffset? LastHeartbeatAt { get; init; }
    public int? BatchTotal { get; init; }
    public int? BatchCompleted { get; init; }
    public int? BatchFailed { get; init; }

    public static RunResponse From(JobRun run) => new()
    {
        Id = run.Id,
        JobName = run.JobName,
        Status = run.Status,
        Arguments = run.Arguments,
        Result = run.Result,
        Error = run.Error,
        Progress = run.Progress,
        CreatedAt = run.CreatedAt,
        StartedAt = run.StartedAt,
        CompletedAt = run.CompletedAt,
        CancelledAt = run.CancelledAt,
        NodeName = run.NodeName,
        Attempt = run.Attempt,
        TraceId = run.TraceId,
        SpanId = run.SpanId,
        ParentRunId = run.ParentRunId,
        RootRunId = run.RootRunId,
        RerunOfRunId = run.RerunOfRunId,
        NotBefore = run.NotBefore,
        NotAfter = run.NotAfter,
        Priority = run.Priority,
        QueuePriority = run.QueuePriority,
        DeduplicationId = run.DeduplicationId,
        LastHeartbeatAt = run.LastHeartbeatAt,
        BatchTotal = run.BatchTotal,
        BatchCompleted = run.BatchCompleted,
        BatchFailed = run.BatchFailed
    };
}

public sealed class PagedResponse<T>
{
    public required IReadOnlyList<T> Items { get; init; }
    public int TotalCount { get; init; }

    public static PagedResponse<TOut> From<TIn, TOut>(PagedResult<TIn> result, Func<TIn, TOut> map) => new()
    {
        Items = result.Items.Select(map).ToList(),
        TotalCount = result.TotalCount
    };
}

public sealed class JobStatsResponse
{
    public int TotalRuns { get; init; }
    public int SucceededRuns { get; init; }
    public int FailedRuns { get; init; }
    public double SuccessRate { get; init; }
    public TimeSpan? AvgDuration { get; init; }
    public DateTimeOffset? LastRunAt { get; init; }

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

public sealed class TimelineBucketResponse
{
    public DateTimeOffset Timestamp { get; init; }
    public int Pending { get; init; }
    public int Running { get; init; }
    public int Completed { get; init; }
    public int Retrying { get; init; }
    public int Cancelled { get; init; }
    public int DeadLetter { get; init; }

    public static TimelineBucketResponse From(TimelineBucket bucket) => new()
    {
        Timestamp = bucket.Start,
        Pending = bucket.Pending,
        Running = bucket.Running,
        Completed = bucket.Completed,
        Retrying = bucket.Retrying,
        Cancelled = bucket.Cancelled,
        DeadLetter = bucket.DeadLetter
    };
}

public sealed class JobResponse
{
    public required string Name { get; init; }
    public string? Description { get; init; }
    public string[] Tags { get; init; } = [];
    public string? CronExpression { get; init; }
    public string? TimeZoneId { get; init; }
    public TimeSpan? Timeout { get; init; }
    public int? MaxConcurrency { get; init; }
    public int Priority { get; init; }
    public RetryPolicy RetryPolicy { get; init; } = new();
    public bool IsContinuous { get; init; }
    public bool IsEnabled { get; init; }
    public string? Queue { get; init; }
    public string? RateLimitName { get; init; }
    public DateTimeOffset? LastHeartbeatAt { get; init; }
    public bool IsActive { get; init; }
    public DateTimeOffset? NextRunAt { get; init; }
    public MisfirePolicy MisfirePolicy { get; init; }

    public JsonElement? ArgumentsSchema { get; init; }

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
            var cron = Cronos.CronExpression.Parse(job.CronExpression);
            var tz = job.TimeZoneId is { } ? TimeZoneInfo.FindSystemTimeZoneById(job.TimeZoneId) : null;
            var next = tz is { }
                ? cron.GetNextOccurrence(now.UtcDateTime, tz)
                : cron.GetNextOccurrence(now.UtcDateTime);
            return next.HasValue ? new DateTimeOffset(next.Value, TimeSpan.Zero) : null;
        }
        catch
        {
            return null;
        }
    }
}

public sealed class NodeResponse
{
    public required string Name { get; init; }
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset LastHeartbeatAt { get; init; }
    public int RunningCount { get; init; }
    public IReadOnlyList<string> RegisteredJobNames { get; init; } = [];
    public bool IsActive { get; init; }

    public IReadOnlyList<string> RegisteredQueueNames { get; init; } = [];

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

public sealed class QueueResponse
{
    public required string Name { get; init; }
    public int Priority { get; init; }
    public int? MaxConcurrency { get; init; }
    public bool IsPaused { get; init; }
    public string? RateLimitName { get; init; }
    public int PendingCount { get; init; }
    public int RunningCount { get; init; }
    public IReadOnlyList<string> ProcessingNodes { get; init; } = [];
}

// Request models

public sealed class TriggerJobRequest
{
    public JsonElement? Args { get; set; }

    public DateTimeOffset? NotBefore { get; set; }
    public DateTimeOffset? NotAfter { get; set; }
    public int? Priority { get; set; }
    public string? DeduplicationId { get; set; }
}

public sealed class UpdateJobRequest
{
    public bool? IsEnabled { get; set; }
}

public sealed class UpdateQueueRequest
{
    public bool? IsPaused { get; set; }
}

public sealed record RunIdResponse(string RunId);