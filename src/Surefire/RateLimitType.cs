namespace Surefire;

/// <summary>
///     Specifies the type of rate limiting window.
/// </summary>
public enum RateLimitType
{
    /// <summary>A fixed window that resets at regular intervals.</summary>
    FixedWindow = 0,

    /// <summary>A sliding window that moves continuously over time.</summary>
    SlidingWindow = 1
}
