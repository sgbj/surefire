namespace Surefire;

/// <summary>
///     Represents a worker node in the Surefire cluster.
/// </summary>
public sealed class NodeInfo
{
    /// <summary>The unique name of the node.</summary>
    public required string Name { get; set; }

    /// <summary>The time the node started.</summary>
    public DateTimeOffset StartedAt { get; set; }

    /// <summary>The time of the node's most recent heartbeat.</summary>
    public DateTimeOffset LastHeartbeatAt { get; set; }

    /// <summary>The number of runs currently executing on this node.</summary>
    public int RunningCount { get; set; }

    /// <summary>The job names registered on this node.</summary>
    public IReadOnlyList<string> RegisteredJobNames { get; set; } = [];

    /// <summary>The queue names registered on this node.</summary>
    public IReadOnlyList<string> RegisteredQueueNames { get; set; } = [];
}