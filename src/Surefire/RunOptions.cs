namespace Surefire;

/// <summary>
///     Options that control how a job run is created and scheduled.
/// </summary>
public sealed record RunOptions
{
    /// <summary>Delays execution until the specified time.</summary>
    public DateTimeOffset? NotBefore { get; init; }

    /// <summary>Automatically cancels the run if it has not started by this time.</summary>
    public DateTimeOffset? NotAfter { get; init; }

    /// <summary>The priority of the run. Higher values are dequeued first.</summary>
    public int? Priority { get; init; }

    /// <summary>
    ///     An application-defined deduplication key scoped per job. If any non-terminal run
    ///     with the same key already exists, the new run is rejected. The key can be reused
    ///     after the existing run reaches a terminal status.
    /// </summary>
    public string? DeduplicationId { get; init; }
}