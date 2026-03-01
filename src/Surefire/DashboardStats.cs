namespace Surefire;

public sealed class DashboardStats
{
    public int TotalJobs { get; set; }
    public int TotalRuns { get; set; }
    public int ActiveRuns { get; set; }
    public double SuccessRate { get; set; }
    public int NodeCount { get; set; }
    public IReadOnlyList<JobRun> RecentRuns { get; set; } = [];
    public Dictionary<string, int> RunsByStatus { get; set; } = new();
    public IReadOnlyList<TimelineBucket> Timeline { get; set; } = [];
}
