namespace Surefire;

/// <summary>
///     Aggregate statistics for a single job definition.
/// </summary>
public sealed record JobStats
{
    /// <summary>The total number of runs for this job.</summary>
    public int TotalRuns { get; init; }

    /// <summary>The number of runs that completed successfully.</summary>
    public int SucceededRuns { get; init; }

    /// <summary>The number of runs that failed permanently.</summary>
    public int FailedRuns { get; init; }

    /// <summary>The ratio of successful runs to total completed runs, from 0.0 to 1.0.</summary>
    public double SuccessRate { get; init; }

    /// <summary>The average duration of completed runs, or null if no runs have completed.</summary>
    public TimeSpan? AvgDuration { get; init; }

    /// <summary>The time of the most recent run, or null if the job has never run.</summary>
    public DateTimeOffset? LastRunAt { get; init; }
}
