using System.Diagnostics;

namespace Surefire;

internal sealed class RunRecord
{
    public required string Id { get; set; }
    public required string JobName { get; set; }
    public JobStatus Status { get; set; }
    public string? Arguments { get; set; }
    public string? Result { get; set; }
    public string? Error { get; set; }
    public double Progress { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? StartedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
    public DateTimeOffset? CancelledAt { get; set; }
    public string? NodeName { get; set; }
    public int Attempt { get; set; }
    public string? TraceId { get; set; }
    public string? SpanId { get; set; }
    public string? ParentRunId { get; set; }
    public string? RootRunId { get; set; }
    public string? RerunOfRunId { get; set; }
    public DateTimeOffset NotBefore { get; set; }
    public DateTimeOffset? NotAfter { get; set; }
    public int Priority { get; set; }
    public int QueuePriority { get; set; }
    public string? DeduplicationId { get; set; }
    public DateTimeOffset? LastHeartbeatAt { get; set; }
    public string? BatchId { get; set; }

    internal JobRun ToPublic() => new()
    {
        Id = Id, JobName = JobName, Status = Status, Arguments = Arguments, Result = Result,
        Error = Error, Progress = Progress, CreatedAt = CreatedAt, StartedAt = StartedAt,
        CompletedAt = CompletedAt, CancelledAt = CancelledAt, NodeName = NodeName, Attempt = Attempt,
        TraceId = TraceId != null ? ActivityTraceId.CreateFromString(TraceId.AsSpan()) : default,
        SpanId = SpanId != null ? ActivitySpanId.CreateFromString(SpanId.AsSpan()) : default,
        ParentRunId = ParentRunId, RootRunId = RootRunId,
        RerunOfRunId = RerunOfRunId, NotBefore = NotBefore, NotAfter = NotAfter,
        Priority = Priority, DeduplicationId = DeduplicationId,
        LastHeartbeatAt = LastHeartbeatAt
    };

    internal JobRun ToPublicWithClient(IJobClientInternal client) => new(client)
    {
        Id = Id, JobName = JobName, Status = Status, Arguments = Arguments, Result = Result,
        Error = Error, Progress = Progress, CreatedAt = CreatedAt, StartedAt = StartedAt,
        CompletedAt = CompletedAt, CancelledAt = CancelledAt, NodeName = NodeName, Attempt = Attempt,
        TraceId = TraceId != null ? ActivityTraceId.CreateFromString(TraceId.AsSpan()) : default,
        SpanId = SpanId != null ? ActivitySpanId.CreateFromString(SpanId.AsSpan()) : default,
        ParentRunId = ParentRunId, RootRunId = RootRunId,
        RerunOfRunId = RerunOfRunId, NotBefore = NotBefore, NotAfter = NotAfter,
        Priority = Priority, DeduplicationId = DeduplicationId,
        LastHeartbeatAt = LastHeartbeatAt, BatchId = BatchId
    };
}

