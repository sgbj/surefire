namespace Surefire;

/// <summary>
///     A delegate representing the next step in the job filter pipeline.
/// </summary>
/// <param name="context">The job execution context.</param>
public delegate Task JobFilterDelegate(JobContext context);

/// <summary>
///     A middleware filter that wraps job execution, enabling cross-cutting concerns
///     such as logging, metrics, or error handling.
/// </summary>
public interface IJobFilter
{
    /// <summary>
    ///     Executes the filter logic and optionally calls <paramref name="next" /> to continue the pipeline.
    /// </summary>
    /// <param name="context">The job execution context.</param>
    /// <param name="next">The next filter or job handler in the pipeline.</param>
    Task InvokeAsync(JobContext context, JobFilterDelegate next);
}
