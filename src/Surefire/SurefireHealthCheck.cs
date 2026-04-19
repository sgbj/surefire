using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Surefire;

internal sealed class SurefireHealthCheck(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options,
    TimeProvider timeProvider) : IHealthCheck
{
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

            var nodes = await store.GetNodesAsync(cancellationToken);
            var localNode =
                nodes.FirstOrDefault(n => string.Equals(n.Name, options.NodeName, StringComparison.Ordinal));
            if (localNode is null)
            {
                return HealthCheckResult.Degraded($"Node '{options.NodeName}' heartbeat not found.");
            }

            // Allow one heartbeat interval plus inactive-threshold tolerance before degrading.
            var maxAge = options.HeartbeatInterval + options.InactiveThreshold;
            var age = timeProvider.GetUtcNow() - localNode.LastHeartbeatAt;
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