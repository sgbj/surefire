namespace Surefire;

public delegate Task JobFilterDelegate(JobContext context);

public interface IJobFilter
{
    Task InvokeAsync(JobContext context, JobFilterDelegate next);
}
