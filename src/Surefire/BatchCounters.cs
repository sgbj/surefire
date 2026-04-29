namespace Surefire;

/// <summary>
///     Tracks the progress of a batch by counting runs in each terminal state.
/// </summary>
/// <param name="Total">The total number of runs in the batch.</param>
/// <param name="Succeeded">The number of runs that completed successfully.</param>
/// <param name="Failed">The number of runs that failed permanently.</param>
/// <param name="Cancelled">The number of runs that were cancelled.</param>
public readonly record struct BatchCounters(int Total, int Succeeded, int Failed, int Cancelled);
