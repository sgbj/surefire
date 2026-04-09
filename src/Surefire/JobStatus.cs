namespace Surefire;

/// <summary>
///     Represents the lifecycle status of a job run.
/// </summary>
public enum JobStatus
{
    /// <summary>The run is waiting to be picked up by a worker.</summary>
    Pending = 0,

    /// <summary>The run is currently executing.</summary>
    Running = 1,

    /// <summary>The run succeeded.</summary>
    Succeeded = 2,

    /// <summary>The run failed and is scheduled for retry.</summary>
    Retrying = 3,

    /// <summary>The run was cancelled.</summary>
    Cancelled = 4,

    /// <summary>The run exhausted all retries and failed permanently.</summary>
    Failed = 5
}

/// <summary>
///     Extension methods for <see cref="JobStatus" />.
/// </summary>
public static class JobStatusExtensions
{
    extension(JobStatus status)
    {
        /// <summary>
        ///     Gets whether the status represents a terminal state (Succeeded, Cancelled, or Failed).
        /// </summary>
        public bool IsTerminal => status is JobStatus.Succeeded or JobStatus.Cancelled or JobStatus.Failed;
    }
}