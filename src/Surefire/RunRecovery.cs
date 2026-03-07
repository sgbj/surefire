using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class RunRecovery(
    IJobStore store,
    INotificationProvider notifications,
    JobRegistry registry,
    TimeProvider timeProvider,
    SurefireOptions options,
    ILogger<RunRecovery> logger)
{
    /// <summary>
    /// Attempts to recover an orphaned run by atomically transitioning it from Running to Failed/DeadLetter.
    /// Returns true if the run was recovered (CAS succeeded), false if another process already recovered it.
    /// </summary>
    internal async Task<bool> TryRecoverRunAsync(JobRun run, string reason, CancellationToken ct = default)
    {
        // Prefer the in-memory registry (matches what JobExecutor uses) to ensure consistent
        // retry decisions. Fall back to the store definition for jobs not registered on this node.
        var registeredJob = registry.Get(run.JobName);
        var retryPolicy = registeredJob?.Definition.RetryPolicy
            ?? (await store.GetJobAsync(run.JobName, ct))?.RetryPolicy
            ?? new RetryPolicy();

        var canRetry = run.Attempt < retryPolicy.MaxAttempts;

        var now = timeProvider.GetUtcNow();
        var newStatus = canRetry ? JobStatus.Failed
            : retryPolicy.MaxAttempts > 1 ? JobStatus.DeadLetter
            : JobStatus.Failed;

        // Build a separate update object for CAS — avoids mutating the original reference,
        // which in the InMemory store IS the stored object (same-reference CAS would always fail).
        var update = new JobRun { Id = run.Id, JobName = run.JobName, Status = newStatus, Error = reason, CompletedAt = now };

        // CAS: only update if still Running — prevents duplicate retries when multiple
        // recovery paths (startup, stale-run, stale-node) race on the same run.
        if (!await store.TryUpdateRunStatusAsync(update, JobStatus.Running, ct))
        {
            logger.LogDebug("Run {RunId} already recovered by another process", run.Id);
            return false;
        }

        // CAS succeeded — update the local reference to reflect the new state
        run.Status = newStatus;
        run.Error = reason;
        run.CompletedAt = now;

        // Create retry run first so retryRunId is available for the status event
        string? retryRunId = null;
        if (canRetry)
        {
            var retryDelay = retryPolicy.GetDelay(run.Attempt);
            var retryNow = timeProvider.GetUtcNow();
            var retryRun = new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = run.JobName,
                Arguments = run.Arguments,
                CreatedAt = retryNow,
                NotBefore = retryNow + retryDelay,
                Attempt = run.Attempt + 1,
                Priority = run.Priority,
                RetryOfRunId = run.RetryOfRunId ?? run.Id
            };
            await store.CreateRunAsync(retryRun, ct);
            await notifications.PublishAsync(NotificationChannels.RunCreated, retryRun.Id, ct);
            retryRunId = retryRun.Id;
            logger.LogInformation("Created retry {Attempt} for recovered run {RunId}", retryRun.Attempt, run.Id);
        }

        // Append status event (includes retryRunId so WatchRunAsync can read it from the store)
        var statusPayload = retryRunId is not null
            ? JsonSerializer.Serialize(new { status = (int)run.Status, error = run.Error, retryRunId }, options.SerializerOptions)
            : JsonSerializer.Serialize(new { status = (int)run.Status, error = run.Error }, options.SerializerOptions);
        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Status,
            Payload = statusPayload,
            CreatedAt = now
        };
        await store.AppendEventAsync(evt, ct);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), "", ct);

        // Notify completion subscribers (pure wakeup signal)
        await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), "", ct);

        return true;
    }
}
