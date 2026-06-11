using System.Net;
using System.Net.Http.Json;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.TestHost;
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

public sealed class DashboardBrowserTokenAuthTests
{
    private const string Token = "test-token-123";

    [Fact]
    public async Task ApiRequest_WithoutCookie_Returns401()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response = await client.GetAsync("/surefire/api/jobs", ct);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task UiRequest_WithoutCookie_RedirectsToLogin()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response = await client.GetAsync("/surefire/runs", ct);
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        var location = response.Headers.Location!.ToString();
        Assert.StartsWith("/surefire/login?returnUrl=", location);
        Assert.Contains(Uri.EscapeDataString("/surefire/runs"), location);
    }

    [Fact]
    public async Task Login_ValidToken_SetsCookieAndRedirects()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response = await client.GetAsync($"/surefire/login?t={Token}", ct);

        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal("/surefire/", response.Headers.Location!.ToString());
        var cookie = GetAuthCookie(response);
        Assert.NotNull(cookie);

        using var authed = new HttpRequestMessage(HttpMethod.Get, "/surefire/api/jobs");
        authed.Headers.Add("Cookie", cookie);
        var apiResponse = await client.SendAsync(authed, ct);
        Assert.Equal(HttpStatusCode.OK, apiResponse.StatusCode);

        using var page = new HttpRequestMessage(HttpMethod.Get, "/surefire/runs");
        page.Headers.Add("Cookie", cookie);
        var pageResponse = await client.SendAsync(page, ct);
        Assert.Equal(HttpStatusCode.OK, pageResponse.StatusCode);
    }

    [Fact]
    public async Task Login_ValidToken_HonorsLocalReturnUrl()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response = await client.GetAsync($"/surefire/login?t={Token}&returnUrl=%2Fsurefire%2Fqueues", ct);
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal("/surefire/queues", response.Headers.Location!.ToString());
    }

    [Theory]
    [InlineData("https://evil.example/")]
    [InlineData("//evil.example")]
    [InlineData(@"/\evil.example")]
    public async Task Login_NonLocalReturnUrl_IsIgnored(string returnUrl)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response =
            await client.GetAsync($"/surefire/login?t={Token}&returnUrl={Uri.EscapeDataString(returnUrl)}", ct);
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal("/surefire/", response.Headers.Location!.ToString());
    }

    [Fact]
    public async Task Login_InvalidToken_StripsTokenAndSetsNoCookie()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response = await client.GetAsync("/surefire/login?t=wrong", ct);
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        var location = response.Headers.Location!.ToString();
        Assert.DoesNotContain("wrong", location);
        Assert.Contains("error=1", location);
        Assert.Null(GetAuthCookie(response));
    }

    [Fact]
    public async Task LoginPage_IsServedAnonymously()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response = await client.GetAsync("/surefire/login", ct);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.StartsWith("text/html", response.Content.Headers.ContentType!.ToString());
        Assert.Contains("token", await response.Content.ReadAsStringAsync(ct));
    }

    [Fact]
    public async Task LoginPage_WhenAuthenticated_RedirectsIntoDashboard()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var cookie = GetAuthCookie(await client.GetAsync($"/surefire/login?t={Token}", ct));

        using var request = new HttpRequestMessage(HttpMethod.Get, "/surefire/login");
        request.Headers.Add("Cookie", cookie!);
        var response = await client.SendAsync(request, ct);
        Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
        Assert.Equal("/surefire/", response.Headers.Location!.ToString());
    }

    [Fact]
    public async Task PostLogin_ValidToken_Returns204WithCookie()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response = await client.PostAsJsonAsync("/surefire/login", new { token = Token }, ct);
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.NotNull(GetAuthCookie(response));
    }

    [Fact]
    public async Task PostLogin_InvalidToken_Returns401WithoutCookie()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        var response = await client.PostAsJsonAsync("/surefire/login", new { token = "wrong" }, ct);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
        Assert.Null(GetAuthCookie(response));
    }

    [Fact]
    public async Task ConfigToken_UsedWhenOptionsTokenIsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync(
            token: null,
            configureBuilder: builder => builder.Configuration["Surefire:Dashboard:BrowserToken"] = "from-config");
        using var client = app.GetTestClient();
        var ok = await client.GetAsync("/surefire/login?t=from-config", ct);
        Assert.Equal(HttpStatusCode.Redirect, ok.StatusCode);
        Assert.NotNull(GetAuthCookie(ok));
    }

    [Fact]
    public async Task DashboardRoot_Unauthenticated_RedirectsToLogin()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();

        var withSlash = await client.GetAsync("/surefire/", ct);
        Assert.Equal(HttpStatusCode.Redirect, withSlash.StatusCode);
        Assert.StartsWith("/surefire/login", withSlash.Headers.Location!.ToString());

        var withoutSlash = await client.GetAsync("/surefire", ct);
        Assert.NotEqual(HttpStatusCode.OK, withoutSlash.StatusCode);
    }

    [Fact]
    public async Task PostLogin_FormContent_IsRejected()
    {
        // Pins the CSRF posture: login accepts only JSON, which cross-site forms cannot send.
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync();
        using var client = app.GetTestClient();
        using var content = new FormUrlEncodedContent([new KeyValuePair<string, string>("token", Token)]);
        var response = await client.PostAsync("/surefire/login", content, ct);
        Assert.Equal(HttpStatusCode.UnsupportedMediaType, response.StatusCode);
        Assert.Null(GetAuthCookie(response));
    }

    [Fact]
    public async Task OptionsToken_WinsOverConfigToken()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync(
            configureBuilder: builder => builder.Configuration["Surefire:Dashboard:BrowserToken"] = "from-config");
        using var client = app.GetTestClient();
        var rejected = await client.GetAsync("/surefire/login?t=from-config", ct);
        Assert.Null(GetAuthCookie(rejected));
        var ok = await client.GetAsync($"/surefire/login?t={Token}", ct);
        Assert.NotNull(GetAuthCookie(ok));
    }

    [Fact]
    public async Task Cookie_FromDifferentToken_IsRejected()
    {
        // A shared key ring makes the old cookie decrypt here, so the 401 comes from the fingerprint check.
        var ct = TestContext.Current.CancellationToken;
        var keyDir = Directory.CreateTempSubdirectory("surefire-auth-test");
        void ShareKeys(WebApplicationBuilder b) => b.Services.AddDataProtection()
            .PersistKeysToFileSystem(keyDir)
            .SetApplicationName("surefire-auth-test");

        await using var oldApp = await CreateAuthAppAsync(token: "old-token", configureBuilder: ShareKeys);
        string? cookie;
        using (var oldClient = oldApp.GetTestClient())
        {
            cookie = GetAuthCookie(await oldClient.GetAsync("/surefire/login?t=old-token", ct));
            Assert.NotNull(cookie);
        }

        await using var newApp = await CreateAuthAppAsync(configureBuilder: ShareKeys);
        using var client = newApp.GetTestClient();
        using var request = new HttpRequestMessage(HttpMethod.Get, "/surefire/api/jobs");
        request.Headers.Add("Cookie", cookie!);
        var response = await client.SendAsync(request, ct);
        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task BlankConfigToken_IsTreatedAsUnset()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync(
            token: null,
            configureBuilder: builder => builder.Configuration["Surefire:Dashboard:BrowserToken"] = "");
        using var client = app.GetTestClient();
        var response = await client.GetAsync("/surefire/login?t=", ct);
        Assert.Null(GetAuthCookie(response));
    }

    [Fact]
    public async Task RootMount_WorksEndToEnd()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync(prefix: "/");
        using var client = app.GetTestClient();

        Assert.Equal(HttpStatusCode.Unauthorized, (await client.GetAsync("/api/jobs", ct)).StatusCode);

        var redirect = await client.GetAsync("/jobs", ct);
        Assert.Equal(HttpStatusCode.Redirect, redirect.StatusCode);
        Assert.StartsWith("/login", redirect.Headers.Location!.ToString());

        var login = await client.GetAsync($"/login?t={Token}", ct);
        var cookie = GetAuthCookie(login);
        Assert.NotNull(cookie);
        Assert.Equal("/", login.Headers.Location!.ToString());

        using var authed = new HttpRequestMessage(HttpMethod.Get, "/api/jobs");
        authed.Headers.Add("Cookie", cookie!);
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(authed, ct)).StatusCode);
    }

    [Fact]
    public async Task SecondMount_Throws()
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Services.AddSurefire(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(10);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
        });
        builder.Services.AddSurefireDashboard(static o => o.BrowserToken = Token);
        await using var app = builder.Build();
        app.MapSurefireDashboard();
        var ex = Assert.Throws<InvalidOperationException>(() => app.MapSurefireDashboard("/admin"));
        Assert.Contains("single mount", ex.Message);
    }

    [Fact]
    public async Task CustomPrefix_WorksEndToEnd()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var app = await CreateAuthAppAsync(prefix: "/admin");
        using var client = app.GetTestClient();

        var unauthorized = await client.GetAsync("/admin/api/jobs", ct);
        Assert.Equal(HttpStatusCode.Unauthorized, unauthorized.StatusCode);

        var redirect = await client.GetAsync("/admin/jobs", ct);
        Assert.StartsWith("/admin/login", redirect.Headers.Location!.ToString());

        var login = await client.GetAsync($"/admin/login?t={Token}", ct);
        var cookie = GetAuthCookie(login);
        Assert.Equal("/admin/", login.Headers.Location!.ToString());

        using var authed = new HttpRequestMessage(HttpMethod.Get, "/admin/api/jobs");
        authed.Headers.Add("Cookie", cookie!);
        Assert.Equal(HttpStatusCode.OK, (await client.SendAsync(authed, ct)).StatusCode);
    }

    internal static string? GetAuthCookie(HttpResponseMessage response)
    {
        if (!response.Headers.TryGetValues("Set-Cookie", out var values))
        {
            return null;
        }

        var setCookie = values.FirstOrDefault(v =>
            v.StartsWith(".Surefire.Dashboard=", StringComparison.Ordinal) &&
            !v.Contains("=;", StringComparison.Ordinal));
        return setCookie?.Split(';')[0];
    }

    internal static async Task<WebApplication> CreateAuthAppAsync(
        string prefix = "/surefire",
        string? token = Token,
        Action<SurefireDashboardOptions>? configureDashboard = null,
        Action<WebApplicationBuilder>? configureBuilder = null)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        // Clear any ambient env token before configureBuilder, which may still set one deliberately.
        builder.Configuration["Surefire:Dashboard:BrowserToken"] = null;
        configureBuilder?.Invoke(builder);
        builder.Services.AddSurefire(options =>
        {
            options.PollingInterval = TimeSpan.FromMilliseconds(10);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(100);
        });
        builder.Services.AddSurefireDashboard(o =>
        {
            if (token is not null)
            {
                o.BrowserToken = token;
            }

            configureDashboard?.Invoke(o);
        });
        var app = builder.Build();
        app.MapSurefireDashboard(prefix);
        await app.StartAsync(TestContext.Current.CancellationToken);
        return app;
    }
}


