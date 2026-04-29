namespace Surefire;

/// <summary>
///     Options that apply to every run produced by a batch operation.
/// </summary>
public sealed record BatchRunOptions
{
    /// <summary>Delays execution until the specified time.</summary>
    public DateTimeOffset? NotBefore { get; init; }

    /// <summary>Automatically cancels the run if it has not started by this time.</summary>
    public DateTimeOffset? NotAfter { get; init; }

    /// <summary>The priority of the run. Higher values are dequeued first.</summary>
    public int? Priority { get; init; }
}
