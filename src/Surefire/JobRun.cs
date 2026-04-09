namespace Surefire;

/// <summary>
///     Represents a snapshot of a job run's state. When returned from <see cref="IJobClient"/>,
///     also provides handle operations for waiting and streaming.
/// </summary>
public sealed class JobRun
{
    private readonly IJobClientInternal? _client;

    internal JobRun() { }
    internal JobRun(IJobClientInternal client) => _client = client;

    /// <summary>Gets the unique identifier of the run.</summary>
    public required string Id { get; init; }

    /// <summary>Gets the name of the job this run belongs to.</summary>
    public required string JobName { get; init; }

    /// <summary>Gets the current status of the run.</summary>
    public JobStatus Status { get; init; }

    /// <summary>Gets the serialized arguments for the run.</summary>
    public string? Arguments { get; init; }

    /// <summary>Gets the serialized result produced by the run.</summary>
    public string? Result { get; init; }

    /// <summary>Gets the error message if the run failed.</summary>
    public string? Error { get; init; }

    /// <summary>Gets the progress of the run, from 0.0 to 1.0.</summary>
    public double Progress { get; init; }

    /// <summary>Gets the time the run was created.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>Gets the time the run started executing.</summary>
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>Gets the time the run completed, failed, or was cancelled.</summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>Gets the time the run was cancelled.</summary>
    public DateTimeOffset? CancelledAt { get; init; }

    /// <summary>Gets the name of the node that claimed this run.</summary>
    public string? NodeName { get; init; }

    /// <summary>Gets the current attempt number. 0 means unclaimed.</summary>
    public int Attempt { get; init; }

    /// <summary>Gets the OpenTelemetry trace ID for distributed tracing.</summary>
    public string? TraceId { get; init; }

    /// <summary>Gets the OpenTelemetry span ID for distributed tracing.</summary>
    public string? SpanId { get; init; }

    /// <summary>Gets the run ID of the parent run (batch coordinator or calling job).</summary>
    public string? ParentRunId { get; init; }

    /// <summary>Gets the run ID of the root ancestor run in a hierarchy.</summary>
    public string? RootRunId { get; init; }

    /// <summary>Gets the run ID that this run is a rerun of.</summary>
    public string? RerunOfRunId { get; init; }

    /// <summary>Gets the earliest time this run may be claimed.</summary>
    public DateTimeOffset NotBefore { get; init; }

    /// <summary>Gets the latest time this run may be claimed. Null means no expiration.</summary>
    public DateTimeOffset? NotAfter { get; init; }

    /// <summary>Gets the priority of the run. Higher values are claimed first.</summary>
    public int Priority { get; init; }

    /// <summary>Gets the denormalized queue priority, captured at creation time.</summary>
    public int QueuePriority { get; init; }

    /// <summary>Gets the deduplication ID used to prevent duplicate runs.</summary>
    public string? DeduplicationId { get; init; }

    /// <summary>Gets the time of the last heartbeat from the node executing this run.</summary>
    public DateTimeOffset? LastHeartbeatAt { get; init; }

    /// <summary>Gets the total number of children in a batch. Null for non-batch runs.</summary>
    public int? BatchTotal { get; init; }

    /// <summary>Gets the number of completed children in a batch. Null for non-batch runs.</summary>
    public int? BatchCompleted { get; init; }

    /// <summary>Gets the number of failed children in a batch. Null for non-batch runs.</summary>
    public int? BatchFailed { get; init; }

    /// <summary>Gets whether the run has reached a terminal status.</summary>
    public bool IsTerminal => Status.IsTerminal;

    /// <summary>Gets whether the run completed successfully.</summary>
    public bool IsSuccess => Status == JobStatus.Succeeded;

    /// <summary>Gets whether the run failed permanently.</summary>
    public bool IsFailure => Status == JobStatus.Failed;

    /// <summary>Gets whether the run was cancelled.</summary>
    public bool IsCancelled => Status == JobStatus.Cancelled;

    /// <summary>Deserializes and returns the typed result. Throws if no result is present.</summary>
    public T GetResult<T>() =>
        Result is null
            ? throw new InvalidOperationException("Run did not produce a result.")
            : System.Text.Json.JsonSerializer.Deserialize<T>(Result)!;

    /// <summary>Attempts to deserialize and return the typed result. Returns false if no result is present.</summary>
    public bool TryGetResult<T>(out T? value)
    {
        if (Result is null)
        {
            value = default;
            return false;
        }
        value = System.Text.Json.JsonSerializer.Deserialize<T>(Result);
        return true;
    }

    /// <summary>Waits for this run to reach a terminal status and returns the updated snapshot.</summary>
    public Task<JobRun> WaitAsync(CancellationToken cancellationToken = default) =>
        GetClient().WaitForRunAsync(Id, cancellationToken);

    /// <summary>Waits for this run to complete and deserializes the typed result.</summary>
    public Task<T> WaitAsync<T>(CancellationToken cancellationToken = default) =>
        GetClient().WaitForRunAsync<T>(Id, cancellationToken);

    /// <summary>Streams output items from this run.</summary>
    public IAsyncEnumerable<T> StreamAsync<T>(CancellationToken cancellationToken = default) =>
        GetClient().StreamRunOutputAsync<T>(Id, cancellationToken);

    private IJobClientInternal GetClient() =>
        _client ?? throw new InvalidOperationException(
            "This JobRun does not have a client reference. Use a JobRun returned from IJobClient to call handle operations.");
}
