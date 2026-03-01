namespace Surefire;

/// <summary>
///     Aggregate statistics for the Surefire dashboard.
/// </summary>
public sealed record DashboardStats
{
    /// <summary>The total number of registered jobs.</summary>
    public int TotalJobs { get; init; }

    /// <summary>The total number of runs across all jobs.</summary>
    public int TotalRuns { get; init; }

    /// <summary>The number of runs currently in a non-terminal state.</summary>
    public int ActiveRuns { get; init; }

    /// <summary>The ratio of successful runs to total completed runs, from 0.0 to 1.0.</summary>
    public double SuccessRate { get; init; }

    /// <summary>The number of active worker nodes.</summary>
    public int NodeCount { get; init; }

    /// <summary>A breakdown of run counts by status name.</summary>
    public IReadOnlyDictionary<string, int> RunsByStatus { get; init; } = new Dictionary<string, int>();

    /// <summary>Time-bucketed counts of runs by status for charting.</summary>
    public IReadOnlyList<TimelineBucket> Timeline { get; init; } = [];
}