namespace Surefire;

/// <summary>
///     A time-bucketed count of runs by status for dashboard timeline charts.
/// </summary>
public sealed record TimelineBucket
{
    /// <summary>The start of the time bucket.</summary>
    public DateTimeOffset Start { get; init; }

    /// <summary>The number of pending runs in this bucket.</summary>
    public int Pending { get; init; }

    /// <summary>The number of running runs in this bucket.</summary>
    public int Running { get; init; }

    /// <summary>The number of runs that succeeded in this bucket.</summary>
    public int Succeeded { get; init; }

    /// <summary>The number of retrying runs in this bucket.</summary>
    public int Retrying { get; init; }

    /// <summary>The number of runs that were cancelled in this bucket.</summary>
    public int Cancelled { get; init; }

    /// <summary>The number of runs that failed permanently in this bucket.</summary>
    public int Failed { get; init; }
}