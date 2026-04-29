namespace Surefire;

/// <summary>
///     Filter criteria for querying job runs.
/// </summary>
public sealed record RunFilter
{
    /// <summary>Filters runs by status.</summary>
    public JobStatus? Status { get; init; }

    /// <summary>Filters runs whose job name matches exactly (case-sensitive).</summary>
    public string? JobName { get; init; }

    /// <summary>
    ///     Filters runs whose job name contains the given substring (case-insensitive).
    ///     Independent of <see cref="JobName" />: when both are set, both must match.
    /// </summary>
    public string? JobNameContains { get; init; }

    /// <summary>Filters runs that are direct children of the specified parent run.</summary>
    public string? ParentRunId { get; init; }

    /// <summary>Filters runs that belong to the specified root run hierarchy.</summary>
    public string? RootRunId { get; init; }

    /// <summary>Filters runs that were last executed on the specified node.</summary>
    public string? NodeName { get; init; }

    /// <summary>Filters runs created after the specified time.</summary>
    public DateTimeOffset? CreatedAfter { get; init; }

    /// <summary>Filters runs created before the specified time.</summary>
    public DateTimeOffset? CreatedBefore { get; init; }

    /// <summary>Filters runs completed strictly after the specified time.</summary>
    public DateTimeOffset? CompletedAfter { get; init; }

    /// <summary>Filters runs whose last heartbeat is before the specified time.</summary>
    public DateTimeOffset? LastHeartbeatBefore { get; init; }

    /// <summary>Filters runs that belong to the specified batch.</summary>
    public string? BatchId { get; init; }

    /// <summary>Filters runs by whether they are in a terminal state.</summary>
    public bool? IsTerminal { get; init; }

    /// <summary>The field to order results by.</summary>
    public RunOrderBy OrderBy { get; init; }
}
