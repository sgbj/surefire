using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Surefire;

internal sealed class SurefireHealthCheck(IJobStore store, SurefireOptions options, TimeProvider timeProvider)
    : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            _ = await store.GetDashboardStatsAsync(cancellationToken: cancellationToken);
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