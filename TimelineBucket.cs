namespace Surefire;

public sealed class TimelineBucket
{
    public DateTimeOffset Timestamp { get; set; }
    public int Completed { get; set; }
    public int Failed { get; set; }
    public int Pending { get; set; }
    public int Running { get; set; }
    public int Cancelled { get; set; }
    public int DeadLetter { get; set; }
    public int Skipped { get; set; }
}
