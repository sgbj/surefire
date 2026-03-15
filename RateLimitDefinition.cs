namespace Surefire;

public enum RateLimitType
{
    FixedWindow = 0,
    SlidingWindow = 1
}

public sealed class RateLimitDefinition
{
    public required string Name { get; set; }
    public RateLimitType Type { get; set; }
    public int MaxPermits { get; set; }
    public TimeSpan Window { get; set; }
}
