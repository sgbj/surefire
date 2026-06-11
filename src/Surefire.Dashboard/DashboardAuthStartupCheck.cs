using Microsoft.Extensions.Hosting;

namespace Surefire.Dashboard;

/// <summary>
///     In HostAuthorization mode, forces the app's endpoints to build during startup so the
///     unprotected-dashboard guard (a route group Finally convention) fails the deploy instead
///     of faulting routing on the first request.
/// </summary>
internal sealed class DashboardAuthStartupCheck(DashboardAuthState state) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var dataSource in state.DataSources ?? [])
        {
            _ = dataSource.Endpoints;
        }

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
