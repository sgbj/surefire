namespace Surefire;

/// <summary>
///     Thrown by the typed wait / stream APIs when a run reaches a non-success terminal status
///     (<see cref="JobStatus.Failed" /> or <see cref="JobStatus.Cancelled" />). Callers that need
///     to distinguish outcomes inspect <see cref="Status" />.
/// </summary>
public sealed class JobRunException : Exception
{
    /// <summary>Creates a new <see cref="JobRunException" /> for a non-success terminal run.</summary>
    /// <param name="runId">The identifier of the run.</param>
    /// <param name="status">The terminal status — <see cref="JobStatus.Failed" /> or <see cref="JobStatus.Cancelled" />.</param>
    /// <param name="reason">The termination reason recorded on the run, if any.</param>
    public JobRunException(string runId, JobStatus status, string? reason)
        : base(reason is null
            ? $"Run '{runId}' {status.ToString().ToLowerInvariant()}."
            : $"Run '{runId}' {status.ToString().ToLowerInvariant()}: {reason}")
    {
        RunId = runId;
        Status = status;
        Reason = reason;
    }

    /// <summary>The identifier of the run.</summary>
    public string RunId { get; }

    /// <summary>The terminal status of the run.</summary>
    public JobStatus Status { get; }

    /// <summary>The termination reason recorded on the run, if any.</summary>
    public string? Reason { get; }
}