namespace Surefire;

public class RunOptions
{
    public DateTimeOffset? NotBefore { get; set; }
    public DateTimeOffset? NotAfter { get; set; }
    public int? Priority { get; set; }
    public string? DeduplicationId { get; set; }
}
