namespace Surefire;

public sealed class JobStats
{
    public int TotalRuns { get; set; }
    public int SucceededRuns { get; set; }
    public int FailedRuns { get; set; }
    public double SuccessRate { get; set; }
    public TimeSpan? AvgDuration { get; set; }
    public DateTimeOffset? LastRunAt { get; set; }
}
