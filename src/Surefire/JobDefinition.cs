namespace Surefire;

public sealed class JobDefinition
{
    public required string Name { get; set; }
    public string? Description { get; set; }
    public string[] Tags { get; set; } = [];
    public string? CronExpression { get; set; }
    public TimeSpan? Timeout { get; set; }
    public int? MaxConcurrency { get; set; }
    public RetryPolicy RetryPolicy { get; set; } = new();
    public bool IsEnabled { get; set; } = true;
}
