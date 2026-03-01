namespace Surefire;

/// <summary>
///     Represents resumable cursor state for batch child output streams.
/// </summary>
public sealed record BatchRunEventCursor
{
    /// <summary>
    ///     The last seen batch child output event ID in global event order.
    /// </summary>
    public long SinceEventId { get; init; }

    /// <summary>
    ///     Per-child last seen event IDs. Streaming resumes with events whose IDs are greater than the stored value.
    /// </summary>
    public required IReadOnlyDictionary<string, long> ChildSinceEventIds { get; init; }

    /// <summary>
    ///     The start cursor that includes all child output events from the beginning.
    /// </summary>
    public static BatchRunEventCursor Start { get; } = new()
    {
        SinceEventId = 0,
        ChildSinceEventIds = new Dictionary<string, long>(StringComparer.Ordinal)
    };
}