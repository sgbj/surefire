namespace Surefire;

public enum RunOrderBy
{
    CreatedAt,
    StartedAt,
    CompletedAt
}

public sealed class RunFilter
{
    public string? JobName { get; set; }
    public bool ExactJobName { get; set; }
    public JobStatus? Status { get; set; }
    public string? NodeName { get; set; }
    public string? ParentRunId { get; set; }
    public string? RetryOfRunId { get; set; }
    public string? RerunOfRunId { get; set; }
    public int Skip { get; set; }
    public int Take { get; set; } = 50;
    public RunOrderBy OrderBy { get; set; } = RunOrderBy.CreatedAt;
    public DateTimeOffset? CreatedAfter { get; set; }
    public DateTimeOffset? CreatedBefore { get; set; }
}
