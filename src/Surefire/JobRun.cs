namespace Surefire;

using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

/// <summary>
///     Immutable snapshot of a job run's state.
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

    /// <summary>Gets the effective queue priority (from queue configuration).</summary>
    public int QueuePriority { get; init; }

    /// <summary>Gets the deduplication ID used to prevent duplicate runs.</summary>
    public string? DeduplicationId { get; init; }

    /// <summary>Gets the serialized arguments for the run.</summary>
    public string? Arguments { get; init; }

    /// <summary>Gets the OpenTelemetry trace ID for distributed tracing.</summary>
    public string? TraceId { get; init; }

    /// <summary>Gets the OpenTelemetry span ID for distributed tracing.</summary>
    public string? SpanId { get; init; }

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

    /// <summary>Gets the error message if the run failed.</summary>
    public string? Error { get; init; }

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

    internal JsonSerializerOptions? SerializerOptions { get; init; }

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
            : ResultSerializer.Deserialize<T>(Result, SerializerOptions);

    /// <summary>Attempts to deserialize and return the typed result. Returns false if no result is present.</summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public bool TryGetResult<T>(out T? value)
    {
        if (Result is null)
        {
            value = default;
            return false;
        }
        value = ResultSerializer.Deserialize<T>(Result, SerializerOptions);
        return true;
    }
}
