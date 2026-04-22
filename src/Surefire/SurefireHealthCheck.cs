using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Surefire;

internal sealed class SurefireHealthCheck(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options,
    TimeProvider timeProvider,
    LoopHealthTracker loopHealth) : IHealthCheck
{
    // Surfaces persistent loop failures through the health check. ConsecutiveFailureThreshold
    // catches a loop that keeps throwing; MissedTickBudget catches a loop that has gone silent
    // (hung on something that ignores cancellation). The budget is multiplied by each loop's own
    // ExpectedCadence so fast loops (executor polling at seconds) alarm quickly while slow loops
    // (retention every few minutes) get proportional room before being flagged.
    internal const int ConsecutiveFailureThreshold = 5;
    internal const int MissedTickBudget = 3;

    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await store.PingAsync(cancellationToken);
            try
            {
                await notifications.PingAsync(cancellationToken);
            }
            catch (Exception ex)
            {
                return HealthCheckResult.Degraded(
                    "Surefire notification health probe failed. Distributed wakeups may be unavailable.",
                    ex);
            }

            // Notification publishing is a separate signal from ping liveness: the server can be
            // reachable while queued publishes fail (auth change, permission revoked, server-side
            // rejection). Treat recent publish failures as a Degraded condition so operators see it.
            var publishHealth = notifications.GetPublishHealth();
            if (publishHealth.LastFailureAt is { } failedAt
                && timeProvider.GetUtcNow() - failedAt < options.HeartbeatInterval)
            {
                return HealthCheckResult.Degraded(
                    $"Surefire notification publish failed {publishHealth.FailureCount} time(s); " +
                    $"most recent at {failedAt:O}. Subscribers may experience elevated wakeup latency.");
            }

            var now = timeProvider.GetUtcNow();
            foreach (var (loop, state) in loopHealth.Snapshot())
            {
                if (state.ConsecutiveFailures >= ConsecutiveFailureThreshold)
                {
                    return HealthCheckResult.Degraded(
                        $"Surefire '{loop}' loop has failed {state.ConsecutiveFailures} consecutive ticks.");
                }

                // Stale-tick degrade: the loop has missed MissedTickBudget consecutive expected
                // ticks without registering a success. Covers hung-loop cases (body stuck waiting
                // on a store call that ignores cancellation) where RecordFailure never fires —
                // ConsecutiveFailures stays at 0 but the silence itself is the signal. Baseline
                // against RegisteredAt until the first success so a loop that wedges on its very
                // first tick still alarms on schedule.
                var staleAfter = state.ExpectedCadence * MissedTickBudget;
                var since = state.LastSuccessAt ?? state.RegisteredAt;
                if (now - since > staleAfter)
                {
                    return HealthCheckResult.Degraded(
                        state.LastSuccessAt is { } lastSuccess
                            ? $"Surefire '{loop}' loop has had no successful tick in {now - lastSuccess} " +
                              $"(threshold {staleAfter})."
                            : $"Surefire '{loop}' loop has not produced a successful tick since registration " +
                              $"{now - state.RegisteredAt} ago (threshold {staleAfter}).");
                }
            }

            var nodes = await store.GetNodesAsync(cancellationToken);
            var localNode =
                nodes.FirstOrDefault(n => string.Equals(n.Name, options.NodeName, StringComparison.Ordinal));
            if (localNode is null)
            {
                return HealthCheckResult.Degraded($"Node '{options.NodeName}' heartbeat not found.");
            }

            // Allow one heartbeat interval plus inactive-threshold tolerance before degrading.
            var maxAge = options.HeartbeatInterval + options.InactiveThreshold;
            var age = now - localNode.LastHeartbeatAt;
            if (age > maxAge)
            {
                return HealthCheckResult.Degraded(
                    $"Node '{options.NodeName}' heartbeat is stale by {age}. Threshold is {maxAge}.");
            }

            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Surefire store health probe failed.", ex);
        }
    }
}