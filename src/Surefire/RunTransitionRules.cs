namespace Surefire;

internal static class RunTransitionRules
{
    public static bool IsAllowed(JobStatus from, JobStatus to) => (from, to) switch
    {
        (JobStatus.Pending, JobStatus.Running) => true,
        (JobStatus.Pending, JobStatus.Cancelled) => true,
        (JobStatus.Running, JobStatus.Pending) => true,
        (JobStatus.Running, JobStatus.Succeeded) => true,
        (JobStatus.Running, JobStatus.Failed) => true,
        (JobStatus.Running, JobStatus.Cancelled) => true,
        _ => false
    };
}
