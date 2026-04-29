namespace Surefire.Tests.Conformance;

public abstract class StoreConformanceBase : IAsyncLifetime
{
    internal IJobStore Store { get; private set; } = null!;

    public async ValueTask InitializeAsync()
    {
        Store = await CreateStoreAsync();
        await Store.MigrateAsync();
    }

    public virtual ValueTask DisposeAsync() => ValueTask.CompletedTask;
    internal abstract Task<IJobStore> CreateStoreAsync();

    internal static JobRun CreateRun(string? jobName = null, JobStatus status = JobStatus.Pending,
        string? id = null)
    {
        // Truncate to milliseconds for cross-store compatibility (Redis uses millisecond precision).
        // Backdate NotBefore slightly to avoid DB/app clock skew causing spurious immediate-claim misses in CI.
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        return new()
        {
            Id = id ?? Guid.CreateVersion7().ToString("N"),
            JobName = jobName ?? "TestJob",
            Status = status,
            CreatedAt = now,
            NotBefore = now.AddSeconds(-1)
        };
    }

    protected static DateTimeOffset TruncateToMilliseconds(DateTimeOffset dt) =>
        new(dt.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond, dt.Offset);

    internal static RunStatusTransition Transition(JobRun run, JobStatus expectedStatus) =>
        (expectedStatus, run.Status) switch
        {
            (JobStatus.Pending, JobStatus.Running) when run.StartedAt.HasValue && run.LastHeartbeatAt.HasValue &&
                                                        run.NodeName is { } => RunStatusTransition.PendingToRunning(
                run.Id, run.Attempt, run.NodeName,
                run.StartedAt.Value, run.LastHeartbeatAt.Value, run.NotBefore, run.Progress, run.Reason, run.Result),

            (JobStatus.Running, JobStatus.Pending)
                => RunStatusTransition.RunningToPending(run.Id, run.Attempt, run.NotBefore, run.Reason, run.Result,
                    run.Progress, run.LastHeartbeatAt),

            (JobStatus.Running, JobStatus.Succeeded) when run.CompletedAt.HasValue
                => RunStatusTransition.RunningToSucceeded(run.Id, run.Attempt, run.CompletedAt.Value,
                    run.NotBefore, run.NodeName, run.Progress, run.Result, run.Reason, run.StartedAt,
                    run.LastHeartbeatAt),

            (JobStatus.Running, JobStatus.Failed) when run.CompletedAt.HasValue
                => RunStatusTransition.RunningToFailed(run.Id, run.Attempt, run.CompletedAt.Value,
                    run.NotBefore, run.NodeName, run.Progress, run.Reason, run.Result, run.StartedAt,
                    run.LastHeartbeatAt),

            (JobStatus.Pending, JobStatus.Cancelled) when run.CompletedAt.HasValue && run.CancelledAt.HasValue
                => RunStatusTransition.ToCancelled(JobStatus.Pending, run.Id, run.Attempt, run.CompletedAt.Value,
                    run.CancelledAt.Value, run.NotBefore, run.NodeName, run.Progress, run.Reason, run.Result,
                    run.StartedAt, run.LastHeartbeatAt),

            (JobStatus.Running, JobStatus.Cancelled) when run.CompletedAt.HasValue && run.CancelledAt.HasValue
                => RunStatusTransition.ToCancelled(JobStatus.Running, run.Id, run.Attempt, run.CompletedAt.Value,
                    run.CancelledAt.Value, run.NotBefore, run.NodeName, run.Progress, run.Reason, run.Result,
                    run.StartedAt, run.LastHeartbeatAt),

            _ => throw new InvalidOperationException(
                $"No valid transition factory for {expectedStatus} -> {run.Status} in conformance helper.")
        };

    internal static RunStatusTransition InvalidTransition(JobRun run, JobStatus expectedStatus) => new()
    {
        RunId = run.Id,
        ExpectedStatus = expectedStatus,
        ExpectedAttempt = run.Attempt,
        NewStatus = run.Status,
        NodeName = run.NodeName,
        StartedAt = run.StartedAt,
        CompletedAt = run.CompletedAt,
        CancelledAt = run.CancelledAt,
        Reason = run.Reason,
        Result = run.Result,
        Progress = run.Progress,
        NotBefore = run.NotBefore,
        LastHeartbeatAt = run.LastHeartbeatAt
    };

    protected static JobDefinition CreateJob(string? name = null) => new() { Name = name ?? "TestJob" };
}
