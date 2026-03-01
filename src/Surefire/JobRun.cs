namespace Surefire;

/// <summary>
///     Represents a single execution of a job. Maintains the same ID across retry attempts.
/// </summary>
public sealed class JobRun
{
    /// <summary>
    ///     Gets or sets the unique identifier of the run.
    /// </summary>
    public required string Id { get; set; }

    /// <summary>
    ///     Gets or sets the name of the job this run belongs to.
    /// </summary>
    public required string JobName { get; set; }

    /// <summary>
    ///     Gets or sets the current status of the run.
    /// </summary>
    public JobStatus Status { get; set; }

    /// <summary>
    ///     Gets or sets the serialized arguments for the run.
    /// </summary>
    public string? Arguments { get; set; }

    /// <summary>
    ///     Gets or sets the serialized result produced by the run.
    /// </summary>
    public string? Result { get; set; }

    /// <summary>
    ///     Gets or sets the error message if the run failed.
    /// </summary>
    public string? Error { get; set; }

    /// <summary>
    ///     Gets or sets the progress of the run, from 0.0 to 1.0.
    /// </summary>
    public double Progress { get; set; }

    /// <summary>
    ///     Gets or sets the time the run was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; set; }

    /// <summary>
    ///     Gets or sets the time the run started executing.
    /// </summary>
    public DateTimeOffset? StartedAt { get; set; }

    /// <summary>
    ///     Gets or sets the time the run completed, failed, or was cancelled.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; set; }

    /// <summary>
    ///     Gets or sets the time the run was cancelled.
    /// </summary>
    public DateTimeOffset? CancelledAt { get; set; }

    /// <summary>
    ///     Gets or sets the name of the node that claimed this run.
    /// </summary>
    public string? NodeName { get; set; }

    /// <summary>
    ///     Gets or sets the current attempt number. 0 means unclaimed.
    /// </summary>
    public int Attempt { get; set; }

    /// <summary>
    ///     Gets or sets the OpenTelemetry trace ID for distributed tracing.
    /// </summary>
    public string? TraceId { get; set; }

    /// <summary>
    ///     Gets or sets the OpenTelemetry span ID for distributed tracing.
    /// </summary>
    public string? SpanId { get; set; }

    /// <summary>
    ///     Gets or sets the run ID of the parent run (batch coordinator or calling job).
    /// </summary>
    public string? ParentRunId { get; set; }

    /// <summary>
    ///     Gets or sets the run ID of the root ancestor run in a hierarchy.
    /// </summary>
    public string? RootRunId { get; set; }

    /// <summary>
    ///     Gets or sets the run ID that this run is a rerun of.
    /// </summary>
    public string? RerunOfRunId { get; set; }

    /// <summary>
    ///     Gets or sets the earliest time this run may be claimed.
    /// </summary>
    public DateTimeOffset NotBefore { get; set; }

    /// <summary>
    ///     Gets or sets the latest time this run may be claimed. Null means no expiration.
    /// </summary>
    public DateTimeOffset? NotAfter { get; set; }

    /// <summary>
    ///     Gets or sets the priority of the run. Higher values are claimed first.
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    ///     Gets or sets the denormalized queue priority, captured at creation time and updated
    ///     when the queue priority changes. Used for efficient claim ordering.
    /// </summary>
    public int QueuePriority { get; set; }

    /// <summary>
    ///     Gets or sets the deduplication ID used to prevent duplicate runs.
    /// </summary>
    public string? DeduplicationId { get; set; }

    /// <summary>
    ///     Gets or sets the time of the last heartbeat from the node executing this run.
    /// </summary>
    public DateTimeOffset? LastHeartbeatAt { get; set; }

    /// <summary>
    ///     Gets or sets the total number of children in a batch. Null for non-batch runs.
    /// </summary>
    public int? BatchTotal { get; set; }

    /// <summary>
    ///     Gets or sets the number of completed children in a batch. Null for non-batch runs.
    /// </summary>
    public int? BatchCompleted { get; set; }

    /// <summary>
    ///     Gets or sets the number of failed children in a batch. Null for non-batch runs.
    /// </summary>
    public int? BatchFailed { get; set; }
}