namespace Surefire;

/// <summary>
///     Thrown when persisted stream input history cannot be replayed safely.
/// </summary>
public sealed class InvalidInputHistoryException : Exception
{
    /// <summary>Initializes a new instance scoped to a corrupt input event.</summary>
    /// <param name="runId">The run whose input history is invalid.</param>
    /// <param name="eventId">The store-assigned input event id.</param>
    /// <param name="eventType">The invalid input event type.</param>
    /// <param name="message">A description of the invalid history.</param>
    public InvalidInputHistoryException(string runId, long eventId, RunEventType eventType, string message)
        : base(BuildMessage(runId, eventId, eventType, message))
    {
        RunId = runId;
        EventId = eventId;
        EventType = eventType;
    }

    /// <summary>Initializes a new instance scoped to a corrupt input event with an inner exception.</summary>
    /// <param name="runId">The run whose input history is invalid.</param>
    /// <param name="eventId">The store-assigned input event id.</param>
    /// <param name="eventType">The invalid input event type.</param>
    /// <param name="message">A description of the invalid history.</param>
    /// <param name="innerException">The exception that caused the invalid history.</param>
    public InvalidInputHistoryException(string runId, long eventId, RunEventType eventType, string message,
        Exception innerException)
        : base(BuildMessage(runId, eventId, eventType, message), innerException)
    {
        RunId = runId;
        EventId = eventId;
        EventType = eventType;
    }

    /// <summary>The run whose input history is invalid.</summary>
    public string RunId { get; }

    /// <summary>The store-assigned input event id.</summary>
    public long EventId { get; }

    /// <summary>The invalid input event type.</summary>
    public RunEventType EventType { get; }

    private static string BuildMessage(string runId, long eventId, RunEventType eventType, string message) =>
        $"Invalid input history for run '{runId}', event {eventId} ({eventType}): {message}";
}
