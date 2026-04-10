namespace Surefire;

internal sealed class BatchRecord
{
    public required string Id { get; set; }
    public JobStatus Status { get; set; }
    public int Total { get; set; }
    public int Succeeded { get; set; }
    public int Failed { get; set; }
    public int Cancelled { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset? CompletedAt { get; set; }
}
