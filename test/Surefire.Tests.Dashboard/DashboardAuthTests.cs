using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using System.Security.Claims;
using Surefire.Dashboard;

namespace Surefire.Tests.Dashboard;

public sealed class DashboardAuthOptionsTests
{
    [Fact]
    public void AuthMode_DefaultsToBrowserToken()
    {
        var options = new SurefireDashboardOptions();
        Assert.Equal(DashboardAuthMode.BrowserToken, options.AuthMode);
    }

    [Fact]
    public void BrowserToken_DefaultsToNull()
    {
        var options = new SurefireDashboardOptions();
        Assert.Null(options.BrowserToken);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void BrowserToken_RejectsEmptyAndWhitespace(string value)
    {
        var options = new SurefireDashboardOptions();
        Assert.Throws<ArgumentException>(() => options.BrowserToken = value);
    }

    [Fact]
    public void BrowserToken_AcceptsExplicitValueAndNullReset()
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
    public void GenerateToken_ProducesUniqueLowercaseHex()
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
    public void TokensEqual_ComparesExactly(string expected, string provided, bool equal)
    {
        Assert.Equal(equal, SurefireDashboardAuthentication.TokensEqual(expected, provided));
    }

    [Fact]
    public void CreatePrincipal_CarriesTokenFingerprint()
    {
        var principal = SurefireDashboardAuthentication.CreatePrincipal("secret");
        Assert.True(principal.Identity?.IsAuthenticated);
        Assert.Equal(SurefireDashboardAuthentication.AuthenticationScheme,
            principal.Identity?.AuthenticationType);
        Assert.Equal(SurefireDashboardAuthentication.Fingerprint("secret"),
            principal.FindFirst(SurefireDashboardAuthentication.TokenClaimType)?.Value);
    }

    [Fact]
    public void Fingerprint_IsStableAndDiffersByToken()
    {
        var fingerprint = SurefireDashboardAuthentication.Fingerprint("secret");
        Assert.Equal(fingerprint, SurefireDashboardAuthentication.Fingerprint("secret"));
        Assert.NotEqual(fingerprint, SurefireDashboardAuthentication.Fingerprint("other"));
        Assert.NotEqual("secret", fingerprint);
    }
}

public sealed class DashboardAuthRegistrationTests
{
    [Fact]
    public async Task BrowserTokenMode_RegistersCookieSchemeAndPolicy()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSurefireDashboard();
        await using var provider = services.BuildServiceProvider();

        var schemes = provider.GetRequiredService<IAuthenticationSchemeProvider>();
        Assert.NotNull(await schemes.GetSchemeAsync(SurefireDashboardAuthentication.AuthenticationScheme));

        var policies = provider.GetRequiredService<IAuthorizationPolicyProvider>();
        var policy = await policies.GetPolicyAsync(SurefireDashboardAuthentication.PolicyName);
        Assert.NotNull(policy);
        Assert.Contains(SurefireDashboardAuthentication.AuthenticationScheme, policy.AuthenticationSchemes);
    }

    [Theory]
    [InlineData(DashboardAuthMode.HostAuthorization)]
    [InlineData(DashboardAuthMode.Unsecured)]
    public async Task NonBrowserTokenModes_DoNotRegisterScheme(DashboardAuthMode mode)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSurefireDashboard(o => o.AuthMode = mode);
        await using var provider = services.BuildServiceProvider();

        var schemes = provider.GetService<IAuthenticationSchemeProvider>();
        if (schemes is not null)
        {
            Assert.Null(await schemes.GetSchemeAsync(SurefireDashboardAuthentication.AuthenticationScheme));
        }
    }
}
