namespace Surefire;

internal interface IJobClientInternal
{
    Task<JobRun> WaitForRunAsync(string runId, CancellationToken cancellationToken);
    Task<T> WaitForRunAsync<T>(string runId, CancellationToken cancellationToken);
    IAsyncEnumerable<T> StreamRunOutputAsync<T>(string runId, CancellationToken cancellationToken);
}
