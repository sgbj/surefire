namespace Surefire;

/// <summary>
///     Determines how missed scheduled firings are handled when a node recovers.
/// </summary>
public enum MisfirePolicy
{
    /// <summary>Discard all missed firings and resume from the next scheduled time.</summary>
    Skip,

    /// <summary>Fire once immediately to catch up, then resume normal scheduling.</summary>
    FireOnce,

    /// <summary>Fire once for every missed occurrence, then resume normal scheduling.</summary>
    FireAll
}
