namespace Surefire;

public sealed class NodeInfo
{
    public required string Name { get; set; }
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset LastHeartbeatAt { get; set; }
    public int RunningCount { get; set; }
    public IReadOnlyList<string> RegisteredJobNames { get; set; } = [];
    public IReadOnlyList<string> RegisteredQueueNames { get; set; } = [];
}
