namespace Surefire;

internal interface IJobClientInternal
{
    Task<JobRun> WaitForRunAsync(string runId, CancellationToken cancellationToken);
    Task<T> WaitForRunAsync<T>(string runId, CancellationToken cancellationToken);
    IAsyncEnumerable<T> StreamRunOutputAsync<T>(string runId, CancellationToken cancellationToken);
    IAsyncEnumerable<RunEvent> StreamRunEventsAsync(string runId, CancellationToken cancellationToken);
    Task CancelRunAsync(string runId, CancellationToken cancellationToken);
    Task<JobRun> RerunRunAsync(string runId, CancellationToken cancellationToken);
    Task<JobBatch> WaitForBatchAsync(string batchId, CancellationToken cancellationToken);
    Task CancelBatchAsync(string batchId, CancellationToken cancellationToken);
    Task<IReadOnlyList<JobRun>> GetBatchRunsAsync(string batchId, CancellationToken cancellationToken);
    IAsyncEnumerable<JobRun> StreamBatchRunsAsync(string batchId, CancellationToken cancellationToken);
    IAsyncEnumerable<T> StreamBatchResultsAsync<T>(string batchId, CancellationToken cancellationToken);
    Task<T[]> GetBatchResultsAsync<T>(string batchId, CancellationToken cancellationToken);
    IAsyncEnumerable<RunEvent> StreamBatchEventsAsync(string batchId, CancellationToken cancellationToken);
}
