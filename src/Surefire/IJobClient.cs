namespace Surefire;

public interface IJobClient
{
    Task<string> TriggerAsync(string jobName, object? args = null, DateTimeOffset? notBefore = null, CancellationToken cancellationToken = default);
    Task CancelAsync(string runId, CancellationToken cancellationToken = default);
    Task<string> RerunAsync(string runId, CancellationToken cancellationToken = default);
    Task<TResult> RunAsync<TResult>(string jobName, object? args = null, CancellationToken cancellationToken = default);
}
