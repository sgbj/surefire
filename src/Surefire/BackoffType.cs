namespace Surefire;

/// <summary>
///     Specifies the backoff strategy used for retry delays.
/// </summary>
public enum BackoffType
{
    /// <summary>Uses the same delay for every retry attempt.</summary>
    Fixed,

    /// <summary>Doubles the delay with each retry attempt, up to a maximum.</summary>
    Exponential
}
