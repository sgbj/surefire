namespace Surefire;

public interface IJobClient
{
    // -- Primitives --

    Task<string> TriggerAsync(string jobName, CancellationToken cancellationToken = default);
    Task<string> TriggerAsync(string jobName, object? args, CancellationToken cancellationToken = default);
    Task<string> TriggerAsync(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default);

    Task<RunResult> WaitAsync(string runId, CancellationToken cancellationToken = default);

    Task CancelAsync(string runId, CancellationToken cancellationToken = default);
    Task<string> RerunAsync(string runId, CancellationToken cancellationToken = default);
    Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default);

    // -- Query --

    Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default);

    // -- Convenience (trigger + wait + follow retries) --

    Task<string> RunAsync(string jobName, CancellationToken cancellationToken = default);
    Task<string> RunAsync(string jobName, object? args, CancellationToken cancellationToken = default);
    Task<string> RunAsync(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default);

    Task<TResult> RunAsync<TResult>(string jobName, CancellationToken cancellationToken = default);
    Task<TResult> RunAsync<TResult>(string jobName, object? args, CancellationToken cancellationToken = default);
    Task<TResult> RunAsync<TResult>(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default);

    IAsyncEnumerable<T> StreamAsync<T>(string jobName, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> StreamAsync<T>(string jobName, object? args, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> StreamAsync<T>(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default);

    // -- Plans (ad-hoc) --

    Task<string> TriggerAsync(PlanBuilder plan, CancellationToken cancellationToken = default);
    Task<string> RunAsync(PlanBuilder plan, CancellationToken cancellationToken = default);
    Task<T> RunAsync<T>(PlanBuilder plan, Step<T> outputStep, CancellationToken cancellationToken = default);

    // -- Signals --

    Task SendSignalAsync(string planRunId, string signalName, object? payload = null, CancellationToken cancellationToken = default);
}
