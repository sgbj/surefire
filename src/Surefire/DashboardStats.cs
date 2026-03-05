namespace Surefire;

public sealed class DashboardStats
{
    public int TotalJobs { get; set; }
    public int TotalRuns { get; set; }
    public int ActiveRuns { get; set; }
    public double SuccessRate { get; set; }
    public int NodeCount { get; set; }
    public IReadOnlyList<JobRun> RecentRuns { get; set; } = [];
    public IReadOnlyDictionary<string, int> RunsByStatus { get; set; } = new Dictionary<string, int>();
    public IReadOnlyList<TimelineBucket> Timeline { get; set; } = [];
}
