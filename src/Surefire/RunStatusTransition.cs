namespace Surefire;

/// <summary>
///     Represents a compare-and-swap run status transition request.
/// </summary>
public sealed class RunStatusTransition
{
    /// <summary>
    ///     Gets or sets the run ID being transitioned.
    /// </summary>
    public required string RunId { get; set; }

    /// <summary>
    ///     Gets or sets the status the run must currently have for the transition to apply.
    /// </summary>
    public JobStatus ExpectedStatus { get; set; }

    /// <summary>
    ///     Gets or sets the attempt value that must match for the transition to apply.
    /// </summary>
    public int ExpectedAttempt { get; set; }

    /// <summary>
    ///     Gets or sets the destination status.
    /// </summary>
    public JobStatus NewStatus { get; set; }

    /// <summary>
    ///     Gets or sets the node ownership value for the transitioned run.
    /// </summary>
    public string? NodeName { get; set; }

    /// <summary>
    ///     Gets or sets the started-at timestamp to apply. Null preserves existing value.
    /// </summary>
    public DateTimeOffset? StartedAt { get; set; }

    /// <summary>
    ///     Gets or sets the completed-at timestamp to apply. Null preserves existing value.
    /// </summary>
    public DateTimeOffset? CompletedAt { get; set; }

    /// <summary>
    ///     Gets or sets the cancelled-at timestamp to apply. Null preserves existing value.
    /// </summary>
    public DateTimeOffset? CancelledAt { get; set; }

    /// <summary>
    ///     Gets or sets the error payload.
    /// </summary>
    public string? Error { get; set; }

    /// <summary>
    ///     Gets or sets the result payload.
    /// </summary>
    public string? Result { get; set; }

    /// <summary>
    ///     Gets or sets the progress value.
    /// </summary>
    public double Progress { get; set; }

    /// <summary>
    ///     Gets or sets the not-before timestamp to apply.
    /// </summary>
    public DateTimeOffset NotBefore { get; set; }

    /// <summary>
    ///     Gets or sets the heartbeat timestamp to apply. Null preserves existing value.
    /// </summary>
    public DateTimeOffset? LastHeartbeatAt { get; set; }

    /// <summary>
    ///     Creates a Pending -> Running transition.
    /// </summary>
    public static RunStatusTransition PendingToRunning(string runId, int expectedAttempt, string nodeName,
        DateTimeOffset startedAt, DateTimeOffset lastHeartbeatAt, DateTimeOffset notBefore,
        double progress = 0, string? error = null, string? result = null) => new()
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
        Error = error,
        Result = result
    };

    /// <summary>
    ///     Creates a Running -> Retrying transition.
    /// </summary>
    public static RunStatusTransition RunningToRetrying(string runId, int expectedAttempt,
        DateTimeOffset notBefore, string? nodeName = null, double progress = 0,
        string? error = null, string? result = null, DateTimeOffset? lastHeartbeatAt = null) => new()
    {
        RunId = runId,
        ExpectedStatus = JobStatus.Running,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Retrying,
        NotBefore = notBefore,
        NodeName = nodeName,
        Progress = progress,
        Error = error,
        Result = result,
        LastHeartbeatAt = lastHeartbeatAt
    };

    /// <summary>
    ///     Creates a Retrying -> Pending transition.
    /// </summary>
    public static RunStatusTransition RetryingToPending(string runId, int expectedAttempt,
        DateTimeOffset notBefore, string? nodeName = null, double progress = 0,
        string? error = null, string? result = null, DateTimeOffset? lastHeartbeatAt = null) => new()
    {
        RunId = runId,
        ExpectedStatus = JobStatus.Retrying,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Pending,
        NotBefore = notBefore,
        NodeName = nodeName,
        Progress = progress,
        Error = error,
        Result = result,
        LastHeartbeatAt = lastHeartbeatAt
    };

    /// <summary>
    ///     Creates a Running -> Succeeded transition.
    /// </summary>
    public static RunStatusTransition RunningToSucceeded(string runId, int expectedAttempt,
        DateTimeOffset completedAt, DateTimeOffset notBefore, string? nodeName = null,
        double progress = 1, string? result = null, string? error = null,
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
        Error = error,
        StartedAt = startedAt,
        LastHeartbeatAt = lastHeartbeatAt
    };

    /// <summary>
    ///     Creates a Running -> Failed transition.
    /// </summary>
    public static RunStatusTransition RunningToFailed(string runId, int expectedAttempt,
        DateTimeOffset completedAt, DateTimeOffset notBefore, string? nodeName = null,
        double progress = 1, string? error = null, string? result = null,
        DateTimeOffset? startedAt = null, DateTimeOffset? lastHeartbeatAt = null) => new()
    {
        RunId = runId,
        ExpectedStatus = JobStatus.Running,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Failed,
        CompletedAt = completedAt,
        NotBefore = notBefore,
        NodeName = nodeName,
        Progress = progress,
        Error = error,
        Result = result,
        StartedAt = startedAt,
        LastHeartbeatAt = lastHeartbeatAt
    };

    /// <summary>
    ///     Creates a transition to Cancelled from Pending, Running, or Retrying.
    /// </summary>
    public static RunStatusTransition ToCancelled(JobStatus expectedStatus, string runId,
        int expectedAttempt, DateTimeOffset completedAt, DateTimeOffset cancelledAt,
        DateTimeOffset notBefore, string? nodeName = null, double progress = 0,
        string? error = null, string? result = null, DateTimeOffset? startedAt = null,
        DateTimeOffset? lastHeartbeatAt = null) => new()
    {
        RunId = runId,
        ExpectedStatus = expectedStatus,
        ExpectedAttempt = expectedAttempt,
        NewStatus = JobStatus.Cancelled,
        CompletedAt = completedAt,
        CancelledAt = cancelledAt,
        NotBefore = notBefore,
        NodeName = nodeName,
        Progress = progress,
        Error = error,
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
        (JobStatus.Running, JobStatus.Retrying) => true,
        (JobStatus.Retrying, JobStatus.Pending) => true,
        (JobStatus.Running, JobStatus.Succeeded) => CompletedAt.HasValue,
        (JobStatus.Running, JobStatus.Failed) => CompletedAt.HasValue,
        (JobStatus.Pending, JobStatus.Cancelled) => CompletedAt.HasValue && CancelledAt.HasValue,
        (JobStatus.Running, JobStatus.Cancelled) => CompletedAt.HasValue && CancelledAt.HasValue,
        (JobStatus.Retrying, JobStatus.Cancelled) => CompletedAt.HasValue && CancelledAt.HasValue,
        _ => false
    };
}