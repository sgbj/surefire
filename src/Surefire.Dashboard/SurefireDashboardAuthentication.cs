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
