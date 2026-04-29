namespace Surefire;

/// <summary>
///     Represents an event emitted during a job run, such as a log message, progress update, or output value.
/// </summary>
public sealed record RunEvent
{
    /// <summary>The store-assigned identifier for this event.</summary>
    public long Id { get; init; }

    /// <summary>The identifier of the run that produced this event.</summary>
    public required string RunId { get; init; }

    /// <summary>The type of event.</summary>
    public RunEventType EventType { get; init; }

    /// <summary>The serialized event payload.</summary>
    public required string Payload { get; init; }

    /// <summary>The time the event was created.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    ///     The retry attempt number that produced this event. <c>0</c> is reserved for
    ///     run-scoped events that are shared across attempts.
    /// </summary>
    public int Attempt { get; init; } = 1;
}
