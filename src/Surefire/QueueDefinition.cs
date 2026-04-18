namespace Surefire;

/// <summary>
///     Defines the configuration of a named queue.
/// </summary>
public sealed class QueueDefinition
{
    /// <summary>
    ///     Gets or sets the unique name of the queue.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    ///     Gets or sets the priority of the queue. Higher values cause runs in this queue to be claimed first.
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of concurrent runs in this queue across all nodes.
    /// </summary>
    public int? MaxConcurrency { get; set; }

    /// <summary>
    ///     Gets or sets whether the queue is paused. Paused queues do not allow new runs to be claimed.
    /// </summary>
    public bool IsPaused { get; set; }

    /// <summary>
    ///     Gets or sets the name of the rate limiter applied to this queue.
    /// </summary>
    public string? RateLimitName { get; set; }

    /// <summary>
    ///     Gets or sets the time of the last heartbeat from a node registering this queue.
    /// </summary>
    public DateTimeOffset? LastHeartbeatAt { get; set; }

    internal QueueDefinition Clone() => new()
    {
        Name = Name,
        Priority = Priority,
        MaxConcurrency = MaxConcurrency,
        IsPaused = IsPaused,
        RateLimitName = RateLimitName,
        LastHeartbeatAt = LastHeartbeatAt
    };
}