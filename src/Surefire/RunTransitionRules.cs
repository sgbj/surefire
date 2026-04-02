namespace Surefire;

internal static class RunTransitionRules
{
    public static bool IsAllowed(JobStatus from, JobStatus to) => (from, to) switch
    {
        (JobStatus.Pending, JobStatus.Running) => true,
        (JobStatus.Pending, JobStatus.Cancelled) => true,
        (JobStatus.Running, JobStatus.Retrying) => true,
        (JobStatus.Running, JobStatus.Completed) => true,
        (JobStatus.Running, JobStatus.DeadLetter) => true,
        (JobStatus.Running, JobStatus.Cancelled) => true,
        (JobStatus.Retrying, JobStatus.Pending) => true,
        (JobStatus.Retrying, JobStatus.Cancelled) => true,
        _ => false
    };
}