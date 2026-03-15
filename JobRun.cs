namespace Surefire;

public sealed class JobRun
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
    public int Attempt { get; set; } = 1;
    public string? TraceId { get; set; }
    public string? SpanId { get; set; }
    public string? ParentRunId { get; set; }
    public string? RetryOfRunId { get; set; }
    public string? RerunOfRunId { get; set; }
    public DateTimeOffset NotBefore { get; set; }
    public DateTimeOffset? NotAfter { get; set; }
    public int Priority { get; set; }
    public string? DeduplicationId { get; set; }
    public DateTimeOffset? LastHeartbeatAt { get; set; }
    public string? PlanRunId { get; set; }
    public string? PlanStepId { get; set; }
    public string? PlanStepName { get; set; }
    public string? PlanGraph { get; set; }

    /// <summary>Creates a retry run from a failed run, inheriting all linkage fields.</summary>
    internal static JobRun CreateRetry(JobRun original, TimeSpan delay, DateTimeOffset now) => new()
    {
        Id = Guid.CreateVersion7().ToString("N"),
        JobName = original.JobName,
        Arguments = original.Arguments,
        CreatedAt = now,
        NotBefore = now + delay,
        Attempt = original.Attempt + 1,
        Priority = original.Priority,
        RetryOfRunId = original.RetryOfRunId ?? original.Id,
        ParentRunId = original.ParentRunId,
        PlanRunId = original.PlanRunId,
        PlanStepId = original.PlanStepId,
        PlanStepName = original.PlanStepName
    };
}
