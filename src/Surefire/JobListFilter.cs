namespace Surefire;

/// <summary>
///     Filter criteria for querying registered job definitions.
/// </summary>
public sealed record JobListFilter
{
    /// <summary>Filters jobs whose name contains the specified value.</summary>
    public string? Name { get; init; }

    /// <summary>Filters jobs that have the specified tag.</summary>
    public string? Tag { get; init; }

    /// <summary>Filters jobs by their enabled/disabled state.</summary>
    public bool? IsEnabled { get; init; }

    /// <summary>Filters jobs that have received a heartbeat after the specified time.</summary>
    public DateTimeOffset? HeartbeatAfter { get; init; }
}
