namespace Surefire;

internal static class RunTransitionRules
{
    public static bool IsAllowed(JobStatus from, JobStatus to) => (from, to) switch
    {
        (JobStatus.Pending, JobStatus.Running) => true,
        (JobStatus.Pending, JobStatus.Canceled) => true,
        (JobStatus.Running, JobStatus.Pending) => true,
        (JobStatus.Running, JobStatus.Succeeded) => true,
        (JobStatus.Running, JobStatus.Failed) => true,
        (JobStatus.Running, JobStatus.Canceled) => true,
        _ => false
    };
}
