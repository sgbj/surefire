namespace Surefire;

/// <summary>
///     Defines a named rate limit that can be referenced by jobs and queues.
/// </summary>
public sealed class RateLimitDefinition
{
    /// <summary>
    ///     Gets or sets the unique name of the rate limit.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    ///     Gets or sets the type of rate limiting window.
    /// </summary>
    public RateLimitType Type { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of permits allowed within the window.
    /// </summary>
    public int MaxPermits { get; set; }

    /// <summary>
    ///     Gets or sets the duration of the rate limiting window.
    /// </summary>
    public TimeSpan Window { get; set; }

    /// <summary>
    ///     Gets or sets the time of the last heartbeat from a node registering this rate limit.
    /// </summary>
    public DateTimeOffset? LastHeartbeatAt { get; set; }
}