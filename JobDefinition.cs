namespace Surefire;

public sealed class JobDefinition
{
    public required string Name { get; set; }
    public string? Description { get; set; }
    public string[] Tags { get; set; } = [];
    public string? CronExpression { get; set; }
    public string? TimeZoneId { get; set; }
    public TimeSpan? Timeout { get; set; }
    public int? MaxConcurrency { get; set; }
    public int Priority { get; set; }
    public RetryPolicy RetryPolicy { get; set; } = new();
    public bool IsContinuous { get; set; }
    public string? Queue { get; set; }
    public string? RateLimitName { get; set; }
    public bool IsEnabled { get; set; } = true;
    public bool IsPlan { get; set; }
    public string? PlanGraph { get; set; }
    public bool IsInternal { get; set; }
    public MisfirePolicy MisfirePolicy { get; set; }
    public string? ArgumentsSchema { get; set; }
    public DateTimeOffset? LastHeartbeatAt { get; set; }
}
