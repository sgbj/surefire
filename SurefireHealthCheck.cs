using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Surefire;

public sealed class SurefireHealthCheck(IJobStore store) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await store.GetNodesAsync(cancellationToken);
            return HealthCheckResult.Healthy();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy(exception: ex);
        }
    }
}

public static class SurefireHealthCheckExtensions
{
    public static IHealthChecksBuilder AddSurefire(this IHealthChecksBuilder builder)
        => builder.AddCheck<SurefireHealthCheck>("surefire");
}
