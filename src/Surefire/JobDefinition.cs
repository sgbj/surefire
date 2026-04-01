namespace Surefire;

/// <summary>
///     Defines the configuration and metadata of a registered job.
/// </summary>
public sealed class JobDefinition
{
    /// <summary>
    ///     Gets or sets the unique name of the job.
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    ///     Gets or sets a human-readable description shown in the dashboard.
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    ///     Gets or sets categorization tags for dashboard filtering.
    /// </summary>
    public string[] Tags { get; set; } = [];

    /// <summary>
    ///     Gets or sets the cron expression for scheduled execution.
    /// </summary>
    public string? CronExpression { get; set; }

    /// <summary>
    ///     Gets or sets the IANA time zone ID for cron evaluation. Null uses UTC.
    /// </summary>
    public string? TimeZoneId { get; set; }

    /// <summary>
    ///     Gets or sets the hard timeout per execution attempt.
    /// </summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of concurrent instances across all nodes.
    /// </summary>
    public int? MaxConcurrency { get; set; }

    /// <summary>
    ///     Gets or sets the default priority for runs of this job. Higher values are claimed first.
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    ///     Gets or sets the retry policy for failed executions.
    /// </summary>
    public RetryPolicy RetryPolicy { get; set; } = new();

    /// <summary>
    ///     Gets or sets whether this job is continuous (self-restarting on completion).
    /// </summary>
    public bool IsContinuous { get; set; }

    /// <summary>
    ///     Gets or sets the name of the queue this job is assigned to.
    /// </summary>
    public string? Queue { get; set; }

    /// <summary>
    ///     Gets or sets the name of the rate limiter applied to this job.
    /// </summary>
    public string? RateLimitName { get; set; }

    /// <summary>
    ///     Gets or sets whether this job is enabled for scheduling and execution.
    /// </summary>
    public bool IsEnabled { get; set; } = true;

    /// <summary>
    ///     Gets or sets how missed scheduled firings are handled when a node recovers.
    /// </summary>
    public MisfirePolicy MisfirePolicy { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of missed occurrences to schedule on each scheduler tick
    ///     when <see cref="MisfirePolicy"/> is <see cref="Surefire.MisfirePolicy.FireAll"/>.
    ///     Null means unlimited.
    /// </summary>
    public int? FireAllLimit { get; set; }

    /// <summary>
    ///     Gets or sets the JSON Schema describing the job's arguments.
    /// </summary>
    public string? ArgumentsSchema { get; set; }

    /// <summary>
    ///     Gets or sets the time of the last heartbeat from a node registering this job.
    /// </summary>
    public DateTimeOffset? LastHeartbeatAt { get; set; }

    /// <summary>
    ///     Gets or sets the time of the last cron fire for this job.
    /// </summary>
    public DateTimeOffset? LastCronFireAt { get; set; }
}