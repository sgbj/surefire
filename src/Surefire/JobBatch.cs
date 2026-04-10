namespace Surefire;

using System.Diagnostics.CodeAnalysis;

/// <summary>Represents a batch of job runs submitted together.</summary>
public sealed class JobBatch
{
    private readonly IJobClientInternal? _client;

    internal JobBatch() { }
    internal JobBatch(IJobClientInternal client) => _client = client;

    // ------------------------------------------------------------------
    // Identity
    // ------------------------------------------------------------------

    /// <summary>Gets the unique identifier of the batch.</summary>
    public required string Id { get; init; }

    /// <summary>Gets when the batch was created.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>Gets the IDs of all runs in the batch.</summary>
    public IReadOnlyList<string> RunIds { get; init; } = [];

    // ------------------------------------------------------------------
    // Live counters — updated in-place as runs complete
    // ------------------------------------------------------------------

    /// <summary>Gets the current status of the batch.</summary>
    public JobStatus Status { get; internal set; }

    /// <summary>Gets the number of succeeded runs so far.</summary>
    public int Succeeded { get; internal set; }

    /// <summary>Gets the number of failed runs so far.</summary>
    public int Failed { get; internal set; }

    /// <summary>Gets the number of cancelled runs so far.</summary>
    public int Cancelled { get; internal set; }

    /// <summary>Gets when the batch completed, failed, or was cancelled. Null while still in progress.</summary>
    public DateTimeOffset? CompletedAt { get; internal set; }

    // ------------------------------------------------------------------
    // Derived state
    // ------------------------------------------------------------------

    /// <summary>Gets whether the batch has reached a terminal status.</summary>
    public bool IsTerminal => Status.IsTerminal;

    /// <summary>Gets whether all runs in the batch completed successfully.</summary>
    public bool IsSuccess => Status == JobStatus.Succeeded;

    // ------------------------------------------------------------------
    // Interaction — delegate to IJobClientInternal
    // ------------------------------------------------------------------

    /// <summary>
    ///     Waits until all runs reach a terminal status and updates live counters (Succeeded,
    ///     Failed, Status, CompletedAt) in-place. Never throws for run failures — check
    ///     <see cref="Failed"/> after awaiting. Throws <see cref="OperationCanceledException"/>
    ///     if the token fires (stops waiting only — does not cancel the runs).
    /// </summary>
    public async Task WaitAsync(CancellationToken cancellationToken = default)
    {
        var updated = await GetClient().WaitForBatchAsync(Id, cancellationToken);
        CopyLiveFields(updated);
    }

    /// <summary>Snapshot fetch of all run records at this moment.</summary>
    public Task<IReadOnlyList<JobRun>> GetRunsAsync(CancellationToken cancellationToken = default) =>
        GetClient().GetBatchRunsAsync(Id, cancellationToken);

    /// <summary>Yields one <see cref="JobRun"/> snapshot per run as each reaches a terminal status.</summary>
    public IAsyncEnumerable<JobRun> StreamRunsAsync(CancellationToken cancellationToken = default) =>
        GetClient().StreamBatchRunsAsync(Id, cancellationToken);

    /// <summary>
    ///     Waits for all runs and returns all results. Throws <see cref="AggregateException"/>
    ///     wrapping <see cref="JobRunFailedException"/> for any failed or cancelled runs.
    /// </summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public Task<T[]> GetResultsAsync<T>(CancellationToken cancellationToken = default) =>
        GetClient().GetBatchResultsAsync<T>(Id, cancellationToken);

    /// <summary>Streams output values across all runs as they are produced (unordered).</summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> StreamResultsAsync<T>(CancellationToken cancellationToken = default) =>
        GetClient().StreamBatchResultsAsync<T>(Id, cancellationToken);

    /// <summary>
    ///     Streams all raw events across all runs in the batch. All other methods compose from this.
    /// </summary>
    public IAsyncEnumerable<RunEvent> StreamEventsAsync(CancellationToken cancellationToken = default) =>
        GetClient().StreamBatchEventsAsync(Id, cancellationToken);

    /// <summary>Cancels all runs in the batch.</summary>
    public Task CancelAsync(CancellationToken cancellationToken = default) =>
        GetClient().CancelBatchAsync(Id, cancellationToken);

    private void CopyLiveFields(JobBatch source)
    {
        Status = source.Status;
        Succeeded = source.Succeeded;
        Failed = source.Failed;
        Cancelled = source.Cancelled;
        CompletedAt = source.CompletedAt;
    }

    private IJobClientInternal GetClient() =>
        _client ?? throw new InvalidOperationException(
            "This JobBatch does not have a client reference. Use a JobBatch returned from IJobClient to call handle operations.");

    internal static JobBatch FromRecord(BatchRecord record, IJobClientInternal client,
        IReadOnlyList<string>? runIds = null) => new(client)
    {
        Id = record.Id, Status = record.Status,
        Succeeded = record.Succeeded, Failed = record.Failed, Cancelled = record.Cancelled,
        CreatedAt = record.CreatedAt, CompletedAt = record.CompletedAt,
        RunIds = runIds ?? []
    };

    internal static JobBatch FromRecord(BatchRecord record) => new()
    {
        Id = record.Id, Status = record.Status,
        Succeeded = record.Succeeded, Failed = record.Failed, Cancelled = record.Cancelled,
        CreatedAt = record.CreatedAt, CompletedAt = record.CompletedAt
    };
}
