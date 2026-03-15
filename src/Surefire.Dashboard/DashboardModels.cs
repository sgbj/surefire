using System.Text.Json;

namespace Surefire.Dashboard;

// Response models

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
    public bool IsPlan { get; init; }
    public string? PlanGraph { get; init; }
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
        IsActive = job.LastHeartbeatAt is not null && job.LastHeartbeatAt >= activeCutoff,
        NextRunAt = ComputeNextRun(job, now),
        IsPlan = job.IsPlan,
        PlanGraph = job.IsPlan ? job.PlanGraph : null,
        MisfirePolicy = job.MisfirePolicy,
        ArgumentsSchema = job.ArgumentsSchema is not null ? JsonSerializer.Deserialize<JsonElement>(job.ArgumentsSchema) : null
    };

    private static DateTimeOffset? ComputeNextRun(JobDefinition job, DateTimeOffset now)
    {
        if (job.CronExpression is null || !job.IsEnabled) return null;
        try
        {
            var cron = Cronos.CronExpression.Parse(job.CronExpression);
            var tz = job.TimeZoneId is not null ? TimeZoneInfo.FindSystemTimeZoneById(job.TimeZoneId) : null;
            var next = tz is not null
                ? cron.GetNextOccurrence(now.UtcDateTime, tz, false)
                : cron.GetNextOccurrence(now.UtcDateTime, false);
            return next.HasValue ? new DateTimeOffset(next.Value, TimeSpan.Zero) : null;
        }
        catch { return null; }
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
