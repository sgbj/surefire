namespace Surefire;

/// <summary>
///     Represents a compare-and-swap run status transition request.
/// </summary>
public sealed class RunStatusTransition
{
    /// <summary>
    ///     Gets or sets the run ID being transitioned.
    /// </summary>
    public required string RunId { get; init; }

    /// <summary>
    ///     Gets or sets the status the run must currently have for the transition to apply.
    /// </summary>
    public JobStatus ExpectedStatus { get; init; }

    /// <summary>
    ///     Gets or sets the attempt value that must match for the transition to apply.
    /// </summary>
    public int ExpectedAttempt { get; init; }

    /// <summary>
    ///     Gets or sets the destination status.
    /// </summary>
    public JobStatus NewStatus { get; init; }

    /// <summary>
    ///     Gets or sets the node ownership value for the transitioned run.
    /// </summary>
    public string? NodeName { get; init; }

    /// <summary>
    ///     Gets or sets the started-at timestamp to apply. Null preserves existing value.
    /// </summary>
    public DateTimeOffset? StartedAt { get; init; }

    /// <summary>
    ///     Gets or sets the completed-at timestamp to apply. Null preserves existing value.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; init; }

    /// <summary>
    ///     Gets or sets the canceled-at timestamp to apply. Null preserves existing value.
    /// </summary>
    public DateTimeOffset? CanceledAt { get; init; }

    /// <summary>
    ///     Gets or sets the termination reason (non-exception causes only).
    /// </summary>
    public string? Reason { get; init; }

    /// <summary>
    ///     Gets or sets the result payload.
    /// </summary>
    public string? Result { get; init; }

    /// <summary>
    ///     Gets or sets the progress value.
    /// </summary>
    public double Progress { get; init; }

    /// <summary>
    ///     Gets or sets the not-before timestamp to apply.
    /// </summary>
    public DateTimeOffset NotBefore { get; init; }

    /// <summary>
    ///     Gets or sets the heartbeat timestamp to apply. Null preserves existing value.
    /// </summary>
    public DateTimeOffset? LastHeartbeatAt { get; init; }

    /// <summary>
    ///     Optional events to append atomically with the transition. Only committed
    ///     if the CAS succeeds. Null or empty means no additional events beyond the
    ///     auto-generated status event.
    /// </summary>
    public IReadOnlyList<RunEvent>? Events { get; init; }

    /// <summary>
    ///     Creates a Pending -> Running transition.
    /// </summary>
    public static RunStatusTransition PendingToRunning(string runId, int expectedAttempt, string nodeName,
        DateTimeOffset startedAt, DateTimeOffset lastHeartbeatAt, DateTimeOffset notBefore,
        double progress = 0, string? reason = null, string? result = null) => new()
    {
        RunId = runId,
        ExpectedStatus = JobStatus.Pending,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Running,
        NodeName = nodeName,
        StartedAt = startedAt,
        LastHeartbeatAt = lastHeartbeatAt,
        NotBefore = notBefore,
        Progress = progress,
        Reason = reason,
        Result = result
    };

    /// <summary>
    ///     Creates a Running -> Pending transition (retry with delay).
    /// </summary>
    public static RunStatusTransition RunningToPending(string runId, int expectedAttempt,
        DateTimeOffset notBefore, string? reason = null, string? result = null,
        double progress = 0, DateTimeOffset? lastHeartbeatAt = null,
        IReadOnlyList<RunEvent>? events = null) => new()
    {
        RunId = runId,
        ExpectedStatus = JobStatus.Running,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Pending,
        NotBefore = notBefore,
        NodeName = null,
        Progress = progress,
        Reason = reason,
        Result = result,
        LastHeartbeatAt = lastHeartbeatAt,
        Events = events
    };

    /// <summary>
    ///     Creates a Running -> Succeeded transition.
    /// </summary>
    public static RunStatusTransition RunningToSucceeded(string runId, int expectedAttempt,
        DateTimeOffset completedAt, DateTimeOffset notBefore, string? nodeName = null,
        double progress = 1, string? result = null, string? reason = null,
        DateTimeOffset? startedAt = null, DateTimeOffset? lastHeartbeatAt = null) => new()
    {
        RunId = runId,
        ExpectedStatus = JobStatus.Running,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Succeeded,
        CompletedAt = completedAt,
        NotBefore = notBefore,
        NodeName = nodeName,
        Progress = progress,
        Result = result,
        Reason = reason,
        StartedAt = startedAt,
        LastHeartbeatAt = lastHeartbeatAt
    };

    /// <summary>
    ///     Creates a Running -> Failed transition.
    /// </summary>
    public static RunStatusTransition RunningToFailed(string runId, int expectedAttempt,
        DateTimeOffset completedAt, DateTimeOffset notBefore, string? nodeName = null,
        double progress = 1, string? reason = null, string? result = null,
        DateTimeOffset? startedAt = null, DateTimeOffset? lastHeartbeatAt = null,
        IReadOnlyList<RunEvent>? events = null) => new()
    {
        RunId = runId,
        ExpectedStatus = JobStatus.Running,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Failed,
        CompletedAt = completedAt,
        NotBefore = notBefore,
        NodeName = nodeName,
        Progress = progress,
        Reason = reason,
        Result = result,
        StartedAt = startedAt,
        LastHeartbeatAt = lastHeartbeatAt,
        Events = events
    };

    /// <summary>
    ///     Creates a transition to Canceled from Pending or Running.
    /// </summary>
    public static RunStatusTransition ToCanceled(JobStatus expectedStatus, string runId,
        int expectedAttempt, DateTimeOffset completedAt, DateTimeOffset canceledAt,
        DateTimeOffset notBefore, string? nodeName = null, double progress = 0,
        string? reason = null, string? result = null, DateTimeOffset? startedAt = null,
        DateTimeOffset? lastHeartbeatAt = null) => new()
    {
        RunId = runId,
        ExpectedStatus = expectedStatus,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Canceled,
        CompletedAt = completedAt,
        CanceledAt = canceledAt,
        NotBefore = notBefore,
        NodeName = nodeName,
        Progress = progress,
        Reason = reason,
        Result = result,
        StartedAt = startedAt,
        LastHeartbeatAt = lastHeartbeatAt
    };

    /// <summary>
    ///     Returns true when the transition payload has the required fields for its status pair.
    /// </summary>
    public bool HasRequiredFields() => (ExpectedStatus, NewStatus) switch
    {
        (JobStatus.Pending, JobStatus.Running) => StartedAt.HasValue && LastHeartbeatAt.HasValue && NodeName is { },
        (JobStatus.Running, JobStatus.Pending) => true,
        (JobStatus.Running, JobStatus.Succeeded) => CompletedAt.HasValue,
        (JobStatus.Running, JobStatus.Failed) => CompletedAt.HasValue,
        (JobStatus.Pending, JobStatus.Canceled) => CompletedAt.HasValue && CanceledAt.HasValue,
        (JobStatus.Running, JobStatus.Canceled) => CompletedAt.HasValue && CanceledAt.HasValue,
        _ => false
    };
}
