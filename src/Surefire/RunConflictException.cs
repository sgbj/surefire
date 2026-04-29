namespace Surefire;

/// <summary>
///     Thrown when a run operation is rejected due to current run state or constraints.
/// </summary>
public sealed class RunConflictException : Exception
{
    /// <summary>Initializes a new instance with the supplied message.</summary>
    /// <param name="message">A description of the conflict.</param>
    public RunConflictException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance scoped to a specific run.</summary>
    /// <param name="runId">The run identifier associated with the conflict.</param>
    /// <param name="message">A description of the conflict.</param>
    public RunConflictException(string runId, string message)
        : base(message) =>
        RunId = runId;

    /// <summary>Initializes a new instance scoped to a specific run with an inner exception.</summary>
    /// <param name="runId">The run identifier associated with the conflict.</param>
    /// <param name="message">A description of the conflict.</param>
    /// <param name="innerException">The exception that caused the conflict.</param>
    public RunConflictException(string runId, string message, Exception innerException)
        : base(message, innerException) =>
        RunId = runId;

    /// <summary>The run identifier associated with the conflict, when available.</summary>
    public string? RunId { get; }
}
