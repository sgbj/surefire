namespace Surefire;

/// <summary>
///     Exception thrown when awaiting a job run that failed.
/// </summary>
/// <param name="runId">The identifier of the failed run.</param>
/// <param name="error">The error message from the failed run, or null.</param>
public sealed class JobRunFailedException : Exception
{
    /// <summary>
    ///     Creates a new failure exception for a completed run in dead-letter state.
    /// </summary>
    public JobRunFailedException(string runId, string? error)
        : base(error ?? $"Job run '{runId}' failed.")
    {
        RunId = runId;
    }

    /// <summary>
    ///     Creates a new failure exception with an inner exception.
    /// </summary>
    public JobRunFailedException(string runId, string message, Exception innerException)
        : base(message, innerException)
    {
        RunId = runId;
    }

    /// <summary>The identifier of the failed run.</summary>
    public string RunId { get; }
}