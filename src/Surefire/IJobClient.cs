namespace Surefire;

public interface IJobClient
{
    Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default);

    Task<string> TriggerAsync(string jobName, object? args = null, CancellationToken cancellationToken = default);
    Task<string> TriggerAsync(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default);

    Task CancelAsync(string runId, CancellationToken cancellationToken = default);
    Task<string> RerunAsync(string runId, CancellationToken cancellationToken = default);

    Task<TResult> RunAsync<TResult>(string jobName, object? args = null, CancellationToken cancellationToken = default);
    Task<TResult> RunAsync<TResult>(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default);

    Task RunAsync(string jobName, object? args = null, CancellationToken cancellationToken = default);
    Task RunAsync(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default);

    IAsyncEnumerable<T> WatchRunAsync<T>(string runId, CancellationToken cancellationToken = default);
}
