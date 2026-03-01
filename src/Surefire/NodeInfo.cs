namespace Surefire;

public enum NodeStatus
{
    Online,
    Offline
}

public sealed class NodeInfo
{
    public required string Name { get; set; }
    public DateTimeOffset StartedAt { get; set; }
    public DateTimeOffset LastHeartbeatAt { get; set; }
    public NodeStatus Status { get; set; } = NodeStatus.Online;
    public int RunningCount { get; set; }
    public IReadOnlyList<string> RegisteredJobNames { get; set; } = [];
}
