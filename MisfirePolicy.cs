namespace Surefire;

public enum MisfirePolicy
{
    /// <summary>
    /// Skip all missed occurrences and resume from the next future occurrence.
    /// This is the default.
    /// </summary>
    Skip = 0,

    /// <summary>
    /// Fire once immediately for all missed occurrences combined, then resume normal schedule.
    /// </summary>
    FireOnce = 1,

    /// <summary>
    /// Fire every missed occurrence (up to a cap) to catch up, then resume normal schedule.
    /// </summary>
    FireAll = 2
}
