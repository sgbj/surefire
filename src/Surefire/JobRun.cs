namespace Surefire;

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

/// <summary>
///     Represents a snapshot of a job run's state. When returned from <see cref="IJobClient"/>,
///     also provides handle operations for waiting, streaming, and control.
/// </summary>
public sealed class JobRun
{
    private readonly IJobClientInternal? _client;

    internal JobRun() { }
    internal JobRun(IJobClientInternal client) => _client = client;

    // ------------------------------------------------------------------
    // Immutable — set at trigger time, never change
    // ------------------------------------------------------------------

    /// <summary>Gets the unique identifier of the run.</summary>
    public required string Id { get; init; }

    /// <summary>Gets the name of the job this run belongs to.</summary>
    public required string JobName { get; init; }

    /// <summary>Gets the time the run was created.</summary>
    public DateTimeOffset CreatedAt { get; init; }

    /// <summary>Gets the earliest time this run may be claimed.</summary>
    public DateTimeOffset NotBefore { get; init; }

    /// <summary>Gets the latest time this run may be claimed. Null means no expiration.</summary>
    public DateTimeOffset? NotAfter { get; init; }

    /// <summary>Gets the priority of the run. Higher values are claimed first.</summary>
    public int Priority { get; init; }

    /// <summary>Gets the deduplication ID used to prevent duplicate runs.</summary>
    public string? DeduplicationId { get; init; }

    /// <summary>Gets the serialized arguments for the run.</summary>
    public string? Arguments { get; init; }

    /// <summary>Gets the OpenTelemetry trace ID for distributed tracing.</summary>
    public ActivityTraceId TraceId { get; init; }

    /// <summary>Gets the OpenTelemetry span ID for distributed tracing.</summary>
    public ActivitySpanId SpanId { get; init; }

    /// <summary>Gets the batch ID if this run is part of a batch, or null otherwise.</summary>
    public string? BatchId { get; init; }

    /// <summary>Gets the run ID of the parent run (calling job in a hierarchy).</summary>
    public string? ParentRunId { get; init; }

    /// <summary>Gets the run ID of the root ancestor run in a hierarchy.</summary>
    public string? RootRunId { get; init; }

    /// <summary>Gets the run ID that this run is a rerun of.</summary>
    public string? RerunOfRunId { get; init; }

    // ------------------------------------------------------------------
    // Live — updated in-place as the run progresses
    // ------------------------------------------------------------------

    /// <summary>Gets the current status of the run.</summary>
    public JobStatus Status { get; internal set; }

    /// <summary>Gets the progress of the run, from 0.0 to 1.0.</summary>
    public double Progress { get; internal set; }

    /// <summary>Gets the error message if the run failed.</summary>
    public string? Error { get; internal set; }

    /// <summary>Gets the current attempt number. 0 means unclaimed.</summary>
    public int Attempt { get; internal set; }

    /// <summary>Gets the serialized result produced by the run. Null until the run completes.</summary>
    public string? Result { get; internal set; }

    /// <summary>Gets the time the run started executing.</summary>
    public DateTimeOffset? StartedAt { get; internal set; }

    /// <summary>Gets the time the run completed, failed, or was cancelled.</summary>
    public DateTimeOffset? CompletedAt { get; internal set; }

    /// <summary>Gets the time the run was cancelled.</summary>
    public DateTimeOffset? CancelledAt { get; internal set; }

    /// <summary>Gets the time of the last heartbeat from the node executing this run.</summary>
    public DateTimeOffset? LastHeartbeatAt { get; internal set; }

    /// <summary>Gets the name of the node that claimed this run.</summary>
    public string? NodeName { get; internal set; }

    // ------------------------------------------------------------------
    // Derived state
    // ------------------------------------------------------------------

    /// <summary>Gets whether the run has reached a terminal status.</summary>
    public bool IsTerminal => Status.IsTerminal;

    /// <summary>Gets whether the run completed successfully.</summary>
    public bool IsSuccess => Status == JobStatus.Succeeded;

    /// <summary>Gets whether the run failed permanently.</summary>
    public bool IsFailure => Status == JobStatus.Failed;

    /// <summary>Gets whether the run was cancelled.</summary>
    public bool IsCancelled => Status == JobStatus.Cancelled;

    // ------------------------------------------------------------------
    // Synchronous result access (only valid after terminal status)
    // ------------------------------------------------------------------

    /// <summary>Deserializes and returns the typed result. Throws if no result is present.</summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public T GetResult<T>() =>
        Result is null
            ? throw new InvalidOperationException("Run did not produce a result.")
            : System.Text.Json.JsonSerializer.Deserialize<T>(Result)!;

    /// <summary>Attempts to deserialize and return the typed result. Returns false if no result is present.</summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
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

    // ------------------------------------------------------------------
    // Interaction — delegate to IJobClientInternal
    // ------------------------------------------------------------------

    /// <summary>
    ///     Waits until the run reaches a terminal status and updates live fields (Status, Error,
    ///     CompletedAt, etc.) in-place. Never throws for run failure — check <see cref="Status"/>
    ///     after awaiting. Throws <see cref="OperationCanceledException"/> if the token fires
    ///     (stops waiting only — does not cancel the run itself).
    /// </summary>
    public async Task WaitAsync(CancellationToken cancellationToken = default)
    {
        var updated = await GetClient().WaitForRunAsync(Id, cancellationToken);
        CopyLiveFields(updated);
    }

    /// <summary>
    ///     Waits for this run to complete and returns the typed result. Transparent about result
    ///     source: works whether the job returned a value or used streaming output.
    ///     Throws <see cref="JobRunFailedException"/> if the run failed or was cancelled.
    ///     Throws <see cref="OperationCanceledException"/> if the token fires (stops waiting;
    ///     does not cancel the run).
    /// </summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public Task<T> GetResultAsync<T>(CancellationToken cancellationToken = default) =>
        GetClient().WaitForRunAsync<T>(Id, cancellationToken);

    /// <summary>
    ///     Streams output values as the run produces them. Transparent about source: works
    ///     whether results come from Output events or a final Result field.
    /// </summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> StreamResultsAsync<T>(CancellationToken cancellationToken = default) =>
        GetClient().StreamRunOutputAsync<T>(Id, cancellationToken);

    /// <summary>
    ///     Streams all raw events (status changes, progress, output, logs) as they occur.
    ///     <see cref="WaitAsync"/>, <see cref="GetResultAsync{T}"/>, and
    ///     <see cref="StreamResultsAsync{T}"/> all compose from this.
    /// </summary>
    public IAsyncEnumerable<RunEvent> StreamEventsAsync(CancellationToken cancellationToken = default) =>
        GetClient().StreamRunEventsAsync(Id, cancellationToken);

    /// <summary>Cancels this run.</summary>
    public Task CancelAsync(CancellationToken cancellationToken = default) =>
        GetClient().CancelRunAsync(Id, cancellationToken);

    /// <summary>Creates a new run based on this one and returns its handle.</summary>
    public Task<JobRun> RerunAsync(CancellationToken cancellationToken = default) =>
        GetClient().RerunRunAsync(Id, cancellationToken);

    private void CopyLiveFields(JobRun source)
    {
        Status = source.Status;
        Progress = source.Progress;
        Error = source.Error;
        Attempt = source.Attempt;
        Result = source.Result;
        StartedAt = source.StartedAt;
        CompletedAt = source.CompletedAt;
        CancelledAt = source.CancelledAt;
        LastHeartbeatAt = source.LastHeartbeatAt;
        NodeName = source.NodeName;
    }

    private IJobClientInternal GetClient() =>
        _client ?? throw new InvalidOperationException(
            "This JobRun does not have a client reference. Use a JobRun returned from IJobClient to call handle operations.");
}
