namespace Surefire;

public sealed class JobListFilter
{
    public string? Name { get; set; }
    public string? Tag { get; set; }
    public bool? IsEnabled { get; set; }

    /// <summary>
    /// When set, excludes jobs whose LastHeartbeatAt is null or before this cutoff.
    /// Used to hide jobs that no active node is handling.
    /// </summary>
    public DateTimeOffset? HeartbeatAfter { get; set; }

    /// <summary>
    /// When true, includes internal jobs in the results. Default is false.
    /// </summary>
    public bool? IncludeInternal { get; set; }
}
