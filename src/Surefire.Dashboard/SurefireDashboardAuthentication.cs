using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;

namespace Surefire.Dashboard;

/// <summary>
///     Names used by the dashboard's built-in browser-token authentication: the cookie
///     authentication scheme registered by <c>AddSurefireDashboard</c> and the authorization
///     policy applied to the dashboard endpoint group. Reference these when composing host
///     policies with the built-in scheme.
/// </summary>
public static class SurefireDashboardAuthentication
{
    /// <summary>The cookie authentication scheme used by browser-token authentication.</summary>
    public const string AuthenticationScheme = "SurefireDashboard";

    /// <summary>The authorization policy applied to the dashboard group in browser-token mode.</summary>
    public const string PolicyName = "SurefireDashboard";

    internal const string TokenClaimType = "surefire:dashboard";

    internal static string GenerateToken()
        => Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(16));

    /// <summary>Constant-time comparison so token checks don't leak timing information.</summary>
    internal static bool TokensEqual(string expected, string provided)
        => CryptographicOperations.FixedTimeEquals(
            Encoding.UTF8.GetBytes(expected),
            Encoding.UTF8.GetBytes(provided));

    /// <summary>
    ///     A SHA-256 fingerprint of the token, stamped into the principal at sign-in and
    ///     re-checked on every request. Rotating the token invalidates every issued cookie.
    /// </summary>
    internal static string Fingerprint(string token)
        => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(token)));

    internal static ClaimsPrincipal CreatePrincipal(string token)
        => new(new ClaimsIdentity(
            [
                new Claim(ClaimTypes.NameIdentifier, AuthenticationScheme),
                new Claim(TokenClaimType, Fingerprint(token))
            ],
            AuthenticationScheme));
}

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
    public ICollection<Microsoft.AspNetCore.Routing.EndpointDataSource>? DataSources { get; set; }
}
