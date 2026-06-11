using Microsoft.AspNetCore.Routing;

namespace Surefire.Dashboard;

/// <summary>
///     Map-time auth state shared between <c>MapSurefireDashboard</c> (which resolves the prefix
///     and token), the cookie events (which need the login path), and the startup login-URL
///     logger. Registered as a singleton by <c>AddSurefireDashboard</c>.
/// </summary>
internal sealed class DashboardAuthState
{
    /// <summary>
    ///     Normalized prefix with no trailing slash; empty string when mounted at root. The
    ///     dashboard supports a single mount per app in browser-token mode, so any second mount
    ///     throws at map time.
    /// </summary>
    public string? Prefix { get; set; }

    public string? Token { get; set; }

    /// <summary>True when the token was auto-generated (safe to log); configured tokens are never logged.</summary>
    public bool TokenGenerated { get; set; }

    /// <summary>
    ///     The app's endpoint data sources, captured at map time in HostAuthorization mode only,
    ///     so the startup check can force endpoint building during StartAsync.
    /// </summary>
    public ICollection<EndpointDataSource>? DataSources { get; set; }
}
