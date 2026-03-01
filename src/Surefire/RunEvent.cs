namespace Surefire;

/// <summary>
///     Represents an event emitted during a job run, such as a log message, progress update, or output value.
/// </summary>
public sealed record RunEvent
{
    /// <summary>The store-assigned identifier for this event.</summary>
    public long Id { get; set; }

    /// <summary>The identifier of the run that produced this event.</summary>
    public required string RunId { get; set; }

    /// <summary>The type of event.</summary>
    public RunEventType EventType { get; set; }

    /// <summary>The serialized event payload.</summary>
    public required string Payload { get; set; }

    /// <summary>The time the event was created.</summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>
    ///     The retry attempt number that produced this event. <c>0</c> is reserved for
    ///     run-scoped events that are shared across attempts.
    /// </summary>
    public int Attempt { get; set; } = 1;
}