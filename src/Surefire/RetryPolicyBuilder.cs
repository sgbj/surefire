namespace Surefire;

/// <summary>
///     Mutable builder for creating an immutable <see cref="RetryPolicy" />.
/// </summary>
public sealed class RetryPolicyBuilder
{
    /// <summary>The maximum number of retries after the initial attempt. Zero means no retries.</summary>
    public int MaxRetries { get; set; } = 0;

    /// <summary>The backoff strategy to use between retries.</summary>
    public BackoffType BackoffType { get; set; }

    /// <summary>The delay before the first retry.</summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>The maximum delay between retries.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Whether to apply jitter to retry delays to decorrelate concurrent retries.</summary>
    public bool Jitter { get; set; } = true;

    /// <summary>
    ///     Builds an immutable <see cref="RetryPolicy" /> from the current builder settings.
    /// </summary>
    /// <returns>The configured retry policy.</returns>
    public RetryPolicy Build()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(MaxRetries);
        if (InitialDelay < TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(InitialDelay), "InitialDelay cannot be negative.");
        }

        if (MaxDelay <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxDelay), "MaxDelay must be greater than zero.");
        }

        if (InitialDelay > MaxDelay)
        {
            throw new ArgumentOutOfRangeException(nameof(InitialDelay),
                "InitialDelay cannot be greater than MaxDelay.");
        }

        return new()
        {
            MaxRetries = MaxRetries,
            BackoffType = BackoffType,
            InitialDelay = InitialDelay,
            MaxDelay = MaxDelay,
            Jitter = Jitter
        };
    }
}