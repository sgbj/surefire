namespace Surefire;

public sealed class QueueDefinition
{
    public required string Name { get; set; }
    public int Priority { get; set; }
    public int? MaxConcurrency { get; set; }
    public bool IsPaused { get; set; }
    public string? RateLimitName { get; set; }
    public DateTimeOffset? LastHeartbeatAt { get; set; }
}
