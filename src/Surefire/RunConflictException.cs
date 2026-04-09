namespace Surefire;

/// <summary>
///     Thrown when a run operation is rejected due to current run state or constraints.
/// </summary>
public sealed class RunConflictException : Exception
{
    public RunConflictException(string message)
        : base(message)
    {
    }

    public RunConflictException(string runId, string message)
        : base(message)
    {
        RunId = runId;
    }

    public RunConflictException(string runId, string message, Exception innerException)
        : base(message, innerException)
    {
        RunId = runId;
    }

    /// <summary>The run identifier associated with the conflict, when available.</summary>
    public string? RunId { get; }
}