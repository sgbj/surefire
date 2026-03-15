namespace Surefire;

public enum JobStatus
{
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    DeadLetter,
    Skipped
}

public static class JobStatusExtensions
{
    extension(JobStatus status)
    {
        public bool IsTerminal => status is JobStatus.Completed or JobStatus.Failed or JobStatus.Cancelled or JobStatus.DeadLetter or JobStatus.Skipped;
    }
}
