namespace Surefire;

/// <summary>
///     Current statistics for a single queue.
/// </summary>
public sealed record QueueStats
{
    /// <summary>The number of runs waiting to be picked up.</summary>
    public int PendingCount { get; init; }

    /// <summary>The number of runs currently executing.</summary>
    public int RunningCount { get; init; }
}