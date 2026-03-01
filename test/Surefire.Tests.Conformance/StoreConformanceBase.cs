namespace Surefire.Tests.Conformance;

public abstract class StoreConformanceBase : IAsyncLifetime
{
    protected IJobStore Store { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        Store = await CreateStoreAsync();
        await Store.MigrateAsync();
    }

    public virtual Task DisposeAsync() => Task.CompletedTask;
    protected abstract Task<IJobStore> CreateStoreAsync();

    protected static JobRun CreateRun(string? jobName = null, JobStatus status = JobStatus.Pending,
        string? id = null)
    {
        // Truncate to milliseconds for cross-store compatibility (Redis uses millisecond precision)
        var now = TruncateToMilliseconds(DateTimeOffset.UtcNow);
        return new()
        {
            Id = id ?? Guid.CreateVersion7().ToString("N"),
            JobName = jobName ?? "TestJob",
            Status = status,
            CreatedAt = now,
            NotBefore = now
        };
    }

    protected static DateTimeOffset TruncateToMilliseconds(DateTimeOffset dt) =>
        new(dt.Ticks / TimeSpan.TicksPerMillisecond * TimeSpan.TicksPerMillisecond, dt.Offset);

    protected static RunStatusTransition Transition(JobRun run, JobStatus expectedStatus) =>
        (expectedStatus, run.Status) switch
        {
            (JobStatus.Pending, JobStatus.Running) when run.StartedAt.HasValue && run.LastHeartbeatAt.HasValue &&
                                                        run.NodeName is { } => RunStatusTransition.PendingToRunning(
                run.Id, run.Attempt, run.NodeName,
                run.StartedAt.Value, run.LastHeartbeatAt.Value, run.NotBefore, run.Progress, run.Error, run.Result),

            (JobStatus.Running, JobStatus.Retrying)
                => RunStatusTransition.RunningToRetrying(run.Id, run.Attempt, run.NotBefore, run.NodeName,
                    run.Progress, run.Error, run.Result, run.LastHeartbeatAt),

            (JobStatus.Retrying, JobStatus.Pending)
                => RunStatusTransition.RetryingToPending(run.Id, run.Attempt, run.NotBefore, run.NodeName,
                    run.Progress, run.Error, run.Result, run.LastHeartbeatAt),

            (JobStatus.Running, JobStatus.Completed) when run.CompletedAt.HasValue
                => RunStatusTransition.RunningToCompleted(run.Id, run.Attempt, run.CompletedAt.Value,
                    run.NotBefore, run.NodeName, run.Progress, run.Result, run.Error, run.StartedAt,
                    run.LastHeartbeatAt),

            (JobStatus.Running, JobStatus.DeadLetter) when run.CompletedAt.HasValue
                => RunStatusTransition.RunningToDeadLetter(run.Id, run.Attempt, run.CompletedAt.Value,
                    run.NotBefore, run.NodeName, run.Progress, run.Error, run.Result, run.StartedAt,
                    run.LastHeartbeatAt),

            (JobStatus.Pending, JobStatus.Cancelled) when run.CompletedAt.HasValue && run.CancelledAt.HasValue
                => RunStatusTransition.ToCancelled(JobStatus.Pending, run.Id, run.Attempt, run.CompletedAt.Value,
                    run.CancelledAt.Value, run.NotBefore, run.NodeName, run.Progress, run.Error, run.Result,
                    run.StartedAt, run.LastHeartbeatAt),

            (JobStatus.Running, JobStatus.Cancelled) when run.CompletedAt.HasValue && run.CancelledAt.HasValue
                => RunStatusTransition.ToCancelled(JobStatus.Running, run.Id, run.Attempt, run.CompletedAt.Value,
                    run.CancelledAt.Value, run.NotBefore, run.NodeName, run.Progress, run.Error, run.Result,
                    run.StartedAt, run.LastHeartbeatAt),

            (JobStatus.Retrying, JobStatus.Cancelled) when run.CompletedAt.HasValue && run.CancelledAt.HasValue
                => RunStatusTransition.ToCancelled(JobStatus.Retrying, run.Id, run.Attempt, run.CompletedAt.Value,
                    run.CancelledAt.Value, run.NotBefore, run.NodeName, run.Progress, run.Error, run.Result,
                    run.StartedAt, run.LastHeartbeatAt),

            _ => throw new InvalidOperationException(
                $"No valid transition factory for {expectedStatus} -> {run.Status} in conformance helper.")
        };

    protected static RunStatusTransition InvalidTransition(JobRun run, JobStatus expectedStatus) => new()
    {
        RunId = run.Id,
        ExpectedStatus = expectedStatus,
        ExpectedAttempt = run.Attempt,
        NewStatus = run.Status,
        NodeName = run.NodeName,
        StartedAt = run.StartedAt,
        CompletedAt = run.CompletedAt,
        CancelledAt = run.CancelledAt,
        Error = run.Error,
        Result = run.Result,
        Progress = run.Progress,
        NotBefore = run.NotBefore,
        LastHeartbeatAt = run.LastHeartbeatAt
    };

    protected static JobDefinition CreateJob(string? name = null) => new() { Name = name ?? "TestJob" };
}