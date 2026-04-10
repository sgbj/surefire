namespace Surefire;

/// <summary>Client for triggering, running, and managing job runs.</summary>
public interface IJobClient
{
    // Primitives
    Task<JobRun> TriggerAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task<JobBatch> TriggerBatchAsync(IEnumerable<BatchItem> runs, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task<JobBatch> TriggerBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    // Sugar
    Task<T> RunAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task RunAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task<T[]> RunBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task RunBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> StreamBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    // Query
    Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<JobRun> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default);
    Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default);

    // Control
    Task CancelAsync(string runId, CancellationToken cancellationToken = default);
    Task CancelBatchAsync(string batchId, CancellationToken cancellationToken = default);
    Task<JobRun> RerunAsync(string runId, CancellationToken cancellationToken = default);

    // Wait / observe
    Task<JobRun> WaitAsync(string runId, CancellationToken cancellationToken = default);
    Task<T> WaitAsync<T>(string runId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<RunObservation> ObserveAsync(string runId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<RunObservation> ObserveAsync(string runId, RunEventCursor cursor, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, RunEventCursor cursor, CancellationToken cancellationToken = default);

    // Batch wait / stream
    Task<JobBatch> WaitBatchAsync(string batchId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<JobRun> WaitEachAsync(string batchId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId, BatchRunEventCursor cursor, CancellationToken cancellationToken = default);
    Task<IReadOnlyList<JobRun>> GetBatchRunsAsync(string batchId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<JobRun> StreamBatchRunsAsync(string batchId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> StreamBatchResultsAsync<T>(string batchId, CancellationToken cancellationToken = default);
    Task<T[]> GetBatchResultsAsync<T>(string batchId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<RunEvent> StreamBatchEventsAsync(string batchId, CancellationToken cancellationToken = default);
}

