namespace Surefire;

/// <summary>Client for triggering, running, and managing job runs.</summary>
public interface IJobClient
{
    // Primitives
    Task<JobRun> TriggerAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task<JobBatch> TriggerManyAsync(IEnumerable<BatchItem> runs, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task<JobBatch> TriggerManyAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    // Sugar
    Task<T> RunAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task RunAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task<T[]> RunManyAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task RunManyAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);
    IAsyncEnumerable<T> StreamManyAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    // Query
    Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<JobRun> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default);
    Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default);

    // Control
    Task CancelAsync(string runId, CancellationToken cancellationToken = default);
    Task CancelBatchAsync(string batchId, CancellationToken cancellationToken = default);
    Task<JobRun> RerunAsync(string runId, CancellationToken cancellationToken = default);
}

