namespace Surefire;

/// <summary>
///     Tracks the progress of a batch by counting runs in each terminal state.
/// </summary>
/// <param name="Total">The total number of runs in the batch.</param>
/// <param name="Completed">The number of runs that completed successfully.</param>
/// <param name="Failed">The number of runs that failed permanently.</param>
public readonly record struct BatchCounters(int Total, int Completed, int Failed);