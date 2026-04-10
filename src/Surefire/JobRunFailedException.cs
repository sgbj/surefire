namespace Surefire;

/// <summary>
///     Exception thrown when awaiting a job run that failed or was cancelled.
/// </summary>
public sealed class JobRunFailedException : Exception
{
    /// <summary>
    ///     Creates a new failure exception for a run that reached a non-successful terminal status.
    /// </summary>
    /// <param name="runId">The identifier of the run.</param>
    /// <param name="status">The terminal status of the run.</param>
    /// <param name="error">The error message, or null.</param>
    public JobRunFailedException(string runId, JobStatus status, string? error)
        : base(error ?? $"Job run '{runId}' {status.ToString().ToLowerInvariant()}.")
    {
        RunId = runId;
        Status = status;
    }

    /// <summary>
    ///     Creates a new failure exception with an inner exception.
    /// </summary>
    public JobRunFailedException(string runId, string message, Exception innerException)
        : base(message, innerException)
    {
        RunId = runId;
        Status = JobStatus.Failed;
    }

    /// <summary>The identifier of the run.</summary>
    public string RunId { get; }

    /// <summary>The terminal status of the run (<see cref="JobStatus.Failed"/> or <see cref="JobStatus.Cancelled"/>).</summary>
    public JobStatus Status { get; }
}