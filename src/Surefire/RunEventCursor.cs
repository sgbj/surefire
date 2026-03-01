namespace Surefire;

/// <summary>
///     Represents a resumable cursor for run event streams.
/// </summary>
public sealed record RunEventCursor
{
    /// <summary>
    ///     The last seen event ID. Observation resumes with events whose IDs are greater than this value.
    /// </summary>
    public required long SinceEventId { get; init; }

    /// <summary>
    ///     The start cursor that includes all events from the beginning.
    /// </summary>
    public static RunEventCursor Start { get; } = new() { SinceEventId = 0 };
}