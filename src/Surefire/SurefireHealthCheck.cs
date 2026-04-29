using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Surefire;

internal sealed class SurefireHealthCheck(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options,
    TimeProvider timeProvider,
    LoopHealthTracker loopHealth) : IHealthCheck
{
    // ConsecutiveFailureThreshold catches a loop that keeps throwing; MissedTickBudget catches
    // a loop gone silent (hung on something that ignores cancellation). The budget multiplies
    // each loop's ExpectedCadence so fast loops alarm quickly while slow loops get proportional
    // room before being flagged.
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

            // Publish health is a separate signal from ping: the server can be reachable while
            // queued publishes fail (auth change, permission revoked). Recent publish failures
            // surface as Degraded so operators see them.
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
                // Five straight failures is a wedged loop. Kubernetes liveness treats Degraded
                // as alive, which would leave a stuck pod running forever; report Unhealthy so
                // the orchestrator restarts us.
                if (state.ConsecutiveFailures >= ConsecutiveFailureThreshold)
                {
                    return HealthCheckResult.Unhealthy(
                        $"Surefire '{loop}' loop has failed {state.ConsecutiveFailures} consecutive ticks.");
                }

                // Catches hung loops where RecordFailure never fires (stuck on a store call that
                // ignores cancellation), so ConsecutiveFailures stays at 0 but the silence is the
                // signal. Baseline against RegisteredAt until the first success so a loop that
                // wedges on its first tick still alarms on schedule.
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
            // Absent or stale heartbeat means this node has stopped participating; other nodes
            // are already reclaiming our runs via stale recovery. Degraded would be actively
            // misleading; report Unhealthy so the orchestrator restarts us.
            if (localNode is null)
            {
                return HealthCheckResult.Unhealthy($"Node '{options.NodeName}' heartbeat not found.");
            }

            // Allow one heartbeat interval plus inactive-threshold tolerance before flagging.
            var maxAge = options.HeartbeatInterval + options.InactiveThreshold;
            var age = now - localNode.LastHeartbeatAt;
            if (age > maxAge)
            {
                return HealthCheckResult.Unhealthy(
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
