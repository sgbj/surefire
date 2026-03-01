namespace Surefire;

public sealed class RunFilter
{
    public string? JobName { get; set; }
    public JobStatus? Status { get; set; }
    public string? NodeName { get; set; }
    public string? ParentRunId { get; set; }
    public string? OriginalRunId { get; set; }
    public int Skip { get; set; }
    public int Take { get; set; } = 50;
    public string OrderBy { get; set; } = "CreatedAt";
    public DateTimeOffset? CreatedAfter { get; set; }
    public DateTimeOffset? CreatedBefore { get; set; }
}
