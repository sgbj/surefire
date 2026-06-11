using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire.Dashboard;

/// <summary>
///     Logs the dashboard login URL once the server has started. Only auto-generated tokens are
///     announced; explicitly configured tokens are secrets and are never logged.
/// </summary>
internal sealed class DashboardLoginUrlLogger(
    DashboardAuthState state,
    IHostApplicationLifetime lifetime,
    IServer server,
    ILogger<DashboardLoginUrlLogger> logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        lifetime.ApplicationStarted.Register(() =>
        {
            // Nothing to announce if the dashboard was registered but never mapped.
            if (state.Token is not { } token || state.Prefix is not { } prefix || !state.TokenGenerated)
            {
                return;
            }

            var address = server.Features.Get<IServerAddressesFeature>()?.Addresses.FirstOrDefault();
            var origin = address?
                .Replace("0.0.0.0", "localhost", StringComparison.Ordinal)
                .Replace("[::]", "localhost", StringComparison.Ordinal)
                .Replace("+", "localhost", StringComparison.Ordinal)
                .TrimEnd('/') ?? "";

            logger.LogInformation("Surefire dashboard: {LoginUrl}", $"{origin}{prefix}/login?t={token}");
        });

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
