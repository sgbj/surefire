using System.Security.Claims;
using Surefire.Dashboard;

namespace Surefire.Tests.Dashboard;

public sealed class DashboardAuthOptionsTests
{
    [Fact]
    public void AuthMode_Defaults_To_BrowserToken()
    {
        var options = new SurefireDashboardOptions();
        Assert.Equal(DashboardAuthMode.BrowserToken, options.AuthMode);
    }

    [Fact]
    public void BrowserToken_Defaults_To_Null()
    {
        var options = new SurefireDashboardOptions();
        Assert.Null(options.BrowserToken);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void BrowserToken_Rejects_Empty_And_Whitespace(string value)
    {
        var options = new SurefireDashboardOptions();
        Assert.Throws<ArgumentException>(() => options.BrowserToken = value);
    }

    [Fact]
    public void BrowserToken_Accepts_Explicit_Value_And_Null_Reset()
    {
        var options = new SurefireDashboardOptions { BrowserToken = "my-token" };
        Assert.Equal("my-token", options.BrowserToken);
        options.BrowserToken = null;
        Assert.Null(options.BrowserToken);
    }
}

public sealed class SurefireDashboardAuthenticationTests
{
    [Fact]
    public void GenerateToken_Produces_32_Char_Lowercase_Hex_And_Unique_Values()
    {
        var first = SurefireDashboardAuthentication.GenerateToken();
        var second = SurefireDashboardAuthentication.GenerateToken();
        Assert.Matches("^[0-9a-f]{32}$", first);
        Assert.NotEqual(first, second);
    }

    [Theory]
    [InlineData("secret", "secret", true)]
    [InlineData("secret", "Secret", false)]
    [InlineData("secret", "secret ", false)]
    [InlineData("secret", "", false)]
    public void TokensEqual_Compares_Exactly(string expected, string provided, bool equal)
    {
        Assert.Equal(equal, SurefireDashboardAuthentication.TokensEqual(expected, provided));
    }

    [Fact]
    public void CreatePrincipal_Is_Authenticated_And_Carries_The_Token_Fingerprint()
    {
        var principal = SurefireDashboardAuthentication.CreatePrincipal("secret");
        Assert.True(principal.Identity?.IsAuthenticated);
        Assert.Equal(SurefireDashboardAuthentication.AuthenticationScheme,
            principal.Identity?.AuthenticationType);
        Assert.Equal(SurefireDashboardAuthentication.Fingerprint("secret"),
            principal.FindFirst(SurefireDashboardAuthentication.TokenClaimType)?.Value);
    }

    [Fact]
    public void Fingerprint_Is_Stable_And_Not_The_Token_Itself()
    {
        var fingerprint = SurefireDashboardAuthentication.Fingerprint("secret");
        Assert.Equal(fingerprint, SurefireDashboardAuthentication.Fingerprint("secret"));
        Assert.NotEqual(fingerprint, SurefireDashboardAuthentication.Fingerprint("other"));
        Assert.DoesNotContain("secret", fingerprint);
    }
}
