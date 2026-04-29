namespace Surefire;

/// <summary>
///     Configures retry behavior for failed job runs.
/// </summary>
public sealed record RetryPolicy
{
    /// <summary>The maximum number of retries after the initial attempt. Zero means no retries.</summary>
    public int MaxRetries { get; init; } = 0;

    /// <summary>The backoff strategy to use between retries.</summary>
    public BackoffType BackoffType { get; init; }

    /// <summary>The delay before the first retry.</summary>
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>The maximum delay between retries.</summary>
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromMinutes(5);

    /// <summary>Whether to apply jitter to retry delays to decorrelate concurrent retries.</summary>
    public bool Jitter { get; init; } = true;
}
