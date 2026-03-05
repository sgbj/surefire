namespace Surefire;

public enum RunEventType : short
{
    Status = 0,
    Log = 1,
    Progress = 2,
    Output = 3,
    OutputComplete = 4,
    Input = 5,
    InputComplete = 6
}

public sealed class RunEvent
{
    public long Id { get; set; }
    public required string RunId { get; set; }
    public RunEventType EventType { get; set; }
    public required string Payload { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
}
