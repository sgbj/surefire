namespace Surefire;

/// <summary>
///     Represents a single observation emitted by <see cref="IJobClient.ObserveAsync" />.
/// </summary>
public sealed record RunObservation
{
    /// <summary>
    ///     The current persisted run snapshot at the time of this observation.
    /// </summary>
    public required JobRun Run { get; init; }

    /// <summary>
    ///     The newly observed event, if one was available.
    /// </summary>
    public RunEvent? Event { get; init; }

    /// <summary>
    ///     The next cursor checkpoint after this observation.
    /// </summary>
    public required RunEventCursor Cursor { get; init; }
}