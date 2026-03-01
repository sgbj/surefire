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

    /// <summary>The run completed successfully.</summary>
    Completed = 2,

    /// <summary>The run failed and is scheduled for retry.</summary>
    Retrying = 3,

    /// <summary>The run was cancelled.</summary>
    Cancelled = 4,

    /// <summary>The run exhausted all retries and was moved to the dead letter queue.</summary>
    DeadLetter = 5
}

/// <summary>
///     Extension methods for <see cref="JobStatus" />.
/// </summary>
public static class JobStatusExtensions
{
    extension(JobStatus status)
    {
        /// <summary>
        ///     Gets whether the status represents a terminal state (Completed, Cancelled, or DeadLetter).
        /// </summary>
        public bool IsTerminal => status is JobStatus.Completed or JobStatus.Cancelled or JobStatus.DeadLetter;
    }
}