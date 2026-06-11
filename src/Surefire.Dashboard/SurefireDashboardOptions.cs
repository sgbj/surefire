namespace Surefire.Dashboard;

/// <summary>
///     Configuration for the Surefire dashboard endpoints. Override defaults via the
///     <c>configure</c> callback on <c>AddSurefireDashboard</c>.
/// </summary>
public sealed class SurefireDashboardOptions
{
    /// <summary>
    ///     Maximum number of runs returned by the <c>/api/runs/{id}/tree</c> endpoint in a single
    ///     response. Trees larger than this come back with <c>Truncated = true</c> and a
    ///     <c>TotalCount</c> reflecting the full size, so the UI can banner instead of silently
    ///     dropping rows. Defaults to <c>50_000</c>; operators with unusually large run trees
    ///     can raise it (at the cost of payload size and query latency).
    /// </summary>
    public int MaxTreeRuns
    {
        get;
        set
        {
            if (value <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(value), "MaxTreeRuns must be greater than zero.");
            }

            field = value;
        }
    } = 50_000;

    /// <summary>
    ///     How the dashboard authenticates callers. Defaults to
    ///     <see cref="DashboardAuthMode.BrowserToken" />, which requires a token to sign in.
    ///     Set <see cref="DashboardAuthMode.HostAuthorization" /> to plug in your app's own
    ///     authentication, or <see cref="DashboardAuthMode.Unsecured" /> to opt out entirely.
    /// </summary>
    public DashboardAuthMode AuthMode { get; set; } = DashboardAuthMode.BrowserToken;

    /// <summary>
    ///     The browser token accepted by <c>{prefix}/login</c> when <see cref="AuthMode" /> is
    ///     <see cref="DashboardAuthMode.BrowserToken" />. When <c>null</c> (the default), the
    ///     token is read from the <c>Surefire:Dashboard:BrowserToken</c> configuration key, and
    ///     if that is also unset a cryptographically random token is generated at startup and its
    ///     login URL logged. Set an explicit token when running multiple replicas behind a load
    ///     balancer so one login works on every node.
    /// </summary>
    public string? BrowserToken
    {
        get;
        set
        {
            if (value is not null && string.IsNullOrWhiteSpace(value))
            {
                throw new ArgumentException("BrowserToken cannot be empty or whitespace.", nameof(value));
            }

            field = value;
        }
    }
}
