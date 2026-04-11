using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;

namespace Surefire;

/// <summary>Client for triggering, running, and managing job runs.</summary>
public interface IJobClient
{
    // Primitives
    Task<JobRun> TriggerAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task<JobBatch> TriggerBatchAsync(IEnumerable<BatchItem> runs, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task<JobBatch> TriggerBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);

    // Sugar
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    Task<T> RunAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task RunAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default);
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    Task<T[]> RunBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);
    Task RunBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default);
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
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
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    Task<T> WaitAsync<T>(string runId, CancellationToken cancellationToken = default);
    [EditorBrowsable(EditorBrowsableState.Never)]
    IAsyncEnumerable<RunObservation> ObserveAsync(string runId, CancellationToken cancellationToken = default);
    [EditorBrowsable(EditorBrowsableState.Never)]
    IAsyncEnumerable<RunObservation> ObserveAsync(string runId, RunEventCursor cursor, CancellationToken cancellationToken = default);
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, CancellationToken cancellationToken = default);
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [EditorBrowsable(EditorBrowsableState.Never)]
    IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, RunEventCursor cursor, CancellationToken cancellationToken = default);

    // Batch wait / stream
    Task<JobBatch> WaitBatchAsync(string batchId, CancellationToken cancellationToken = default);
    IAsyncEnumerable<JobRun> WaitEachAsync(string batchId, CancellationToken cancellationToken = default);
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId, CancellationToken cancellationToken = default);
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [EditorBrowsable(EditorBrowsableState.Never)]
    IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId, BatchEventCursor cursor, CancellationToken cancellationToken = default);
    [EditorBrowsable(EditorBrowsableState.Never)]
    IAsyncEnumerable<RunEvent> StreamBatchEventsAsync(string batchId, CancellationToken cancellationToken = default);
}

