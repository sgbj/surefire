using Microsoft.Extensions.Logging;

namespace Surefire;

public sealed class RunLogEntry
{
    public required string RunId { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public LogLevel Level { get; set; }
    public required string Message { get; set; }
    public string? Category { get; set; }
}
