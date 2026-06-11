namespace Surefire.Dashboard;

/// <summary>
///     How the Surefire dashboard authenticates browsers and API callers. See
///     <see cref="SurefireDashboardOptions.AuthMode" />.
/// </summary>
public enum DashboardAuthMode
{
    /// <summary>
    ///     Secure by default: the dashboard requires a browser token exchanged for a cookie at
    ///     <c>{prefix}/login</c>. The token comes from <see cref="SurefireDashboardOptions.BrowserToken" />,
    ///     the <c>Surefire:Dashboard:BrowserToken</c> configuration key, or is generated at startup
    ///     (in which case the login URL is logged).
    /// </summary>
    BrowserToken,

    /// <summary>
    ///     The host application owns authentication. The dashboard applies no auth of its own;
    ///     chain <c>.RequireAuthorization(...)</c> on the builder returned by
    ///     <c>MapSurefireDashboard</c> or configure a global fallback policy. If neither protects
    ///     the dashboard, startup fails with a descriptive exception instead of serving the
    ///     dashboard unprotected.
    /// </summary>
    HostAuthorization,

    /// <summary>
    ///     No authentication at all, even when the host has a fallback policy. An explicit opt-out
    ///     for local development; a warning is logged when the dashboard is mapped.
    ///     AllowAnonymous is applied to every dashboard endpoint, so authorization policies chained
    ///     on the returned builder have no effect in this mode.
    /// </summary>
    Unsecured
}
