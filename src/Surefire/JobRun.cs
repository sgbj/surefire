using System.Text.Json.Serialization;

namespace Surefire;

/// <summary>
///     Immutable snapshot of a job run's state. Pure data — hydration lives on
///     <see cref="IJobClient.WaitAsync{T}(string, System.Threading.CancellationToken)" /> /
///     <see cref="IJobClient.StreamAsync{T}(string, System.Threading.CancellationToken)" />.
/// </summary>
public sealed record JobRun
{
    // ------------------------------------------------------------------
    // Identity / immutable — set at trigger time, never change
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

    /// <summary>
    ///     Gets this run's own OpenTelemetry trace ID. Null until the run is first executed; set once
    ///     the executor starts its activity and never changed thereafter.
    /// </summary>
    public string? TraceId { get; init; }

    /// <summary>
    ///     Gets this run's own OpenTelemetry span ID. Null until the run is first executed; set once
    ///     the executor starts its activity and never changed thereafter.
    /// </summary>
    public string? SpanId { get; init; }

    /// <summary>
    ///     Gets the parent OpenTelemetry trace ID captured from the ambient activity at trigger time.
    ///     Used as the parent context when the executor starts this run's activity. Null when the run
    ///     was triggered without an ambient activity.
    /// </summary>
    public string? ParentTraceId { get; init; }

    /// <summary>
    ///     Gets the parent OpenTelemetry span ID captured from the ambient activity at trigger time.
    ///     Used as the parent context when the executor starts this run's activity. Null when the run
    ///     was triggered without an ambient activity.
    /// </summary>
    public string? ParentSpanId { get; init; }

    /// <summary>Gets the batch ID if this run is part of a batch, or null otherwise.</summary>
    public string? BatchId { get; init; }

    /// <summary>Gets the run ID of the parent run (calling job in a hierarchy).</summary>
    public string? ParentRunId { get; init; }

    /// <summary>Gets the run ID of the root ancestor run in a hierarchy.</summary>
    public string? RootRunId { get; init; }

    /// <summary>Gets the run ID that this run is a rerun of.</summary>
    public string? RerunOfRunId { get; init; }

    // ------------------------------------------------------------------
    // Live — set via `with` when run state changes
    // ------------------------------------------------------------------

    /// <summary>Gets the current status of the run.</summary>
    public JobStatus Status { get; init; }

    /// <summary>Gets the progress of the run, from 0.0 to 1.0.</summary>
    public double Progress { get; init; }

    /// <summary>
    ///     Gets the termination reason for this run, only for non-exception causes such as
    ///     client-requested cancellation, expiration past the <see cref="NotAfter" /> deadline,
    ///     missing handler registration, or shutdown interruption. Null when the run terminated
    ///     by exception (retry exhaustion) — per-attempt exception detail lives on
    ///     <see cref="RunEventType.AttemptFailure" /> events.
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>Gets the current attempt number. 0 means unclaimed.</summary>
    public int Attempt { get; init; }

    /// <summary>Gets the serialized result produced by the run. Null until the run completes.</summary>
    public string? Result { get; init; }

    /// <summary>Gets the time the run started executing.</summary>
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>Gets the time the run completed, failed, or was cancelled.</summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>Gets the time the run was cancelled.</summary>
    public DateTimeOffset? CancelledAt { get; init; }

    /// <summary>Gets the time of the last heartbeat from the node executing this run.</summary>
    public DateTimeOffset? LastHeartbeatAt { get; init; }

    /// <summary>Gets the name of the node that claimed this run.</summary>
    public string? NodeName { get; init; }

    // ------------------------------------------------------------------
    // Derived state — excluded from wire serialization; consumers compute from Status.
    // ------------------------------------------------------------------

    /// <summary>Gets whether the run has reached a terminal status.</summary>
    [JsonIgnore]
    public bool IsTerminal => Status.IsTerminal;

    /// <summary>Gets whether the run completed successfully.</summary>
    [JsonIgnore]
    public bool IsSuccess => Status == JobStatus.Succeeded;

    /// <summary>Gets whether the run failed permanently.</summary>
    [JsonIgnore]
    public bool IsFailure => Status == JobStatus.Failed;

    /// <summary>Gets whether the run was cancelled.</summary>
    [JsonIgnore]
    public bool IsCancelled => Status == JobStatus.Cancelled;
}