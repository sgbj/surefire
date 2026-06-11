using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Surefire.Dashboard;

/// <summary>JSON body for <c>POST {prefix}/login</c>.</summary>
internal sealed record LoginRequest(string? Token);

/// <summary>
///     Map-time auth wiring for <c>MapSurefireDashboard</c>. Applies the configured
///     <see cref="DashboardAuthMode" /> to the dashboard route group.
/// </summary>
internal static class DashboardAuthEndpoints
{
    internal static void Apply(IEndpointRouteBuilder endpoints, RouteGroupBuilder group,
        SurefireDashboardOptions options, string normalizedPrefix)
    {
        switch (options.AuthMode)
        {
            case DashboardAuthMode.BrowserToken:
                MapBrowserTokenAuth(endpoints, group, options, normalizedPrefix);
                break;
            case DashboardAuthMode.HostAuthorization:
                AddHostAuthorizationGuard(endpoints, group);
                break;
            case DashboardAuthMode.Unsecured:
                group.AllowAnonymous();
                endpoints.ServiceProvider.GetRequiredService<ILoggerFactory>()
                    .CreateLogger("Surefire.Dashboard")
                    .LogWarning(
                        "Surefire dashboard authentication is disabled (DashboardAuthMode.Unsecured). " +
                        "Anyone who can reach {Prefix} can view job data and trigger, cancel, or rerun jobs.",
                        normalizedPrefix.Length > 0 ? normalizedPrefix : "/");
                break;
        }
    }

    private static void MapBrowserTokenAuth(IEndpointRouteBuilder endpoints, RouteGroupBuilder group,
        SurefireDashboardOptions options, string normalizedPrefix)
    {
        var state = endpoints.ServiceProvider.GetRequiredService<DashboardAuthState>();
        if (state.Prefix is { } existing)
        {
            // A repeat mount would register duplicate routes and point challenges at one prefix.
            throw new InvalidOperationException(
                $"MapSurefireDashboard was already called (prefix '{existing}'). " +
                "The dashboard supports a single mount per application in BrowserToken mode.");
        }

        var configured = options.BrowserToken
                         ?? NonEmpty(endpoints.ServiceProvider.GetService<IConfiguration>()
                             ?["Surefire:Dashboard:BrowserToken"]);
        state.Token = configured ?? SurefireDashboardAuthentication.GenerateToken();
        state.TokenGenerated = configured is null;
        state.Prefix = normalizedPrefix;
        var token = state.Token;

        group.RequireAuthorization(SurefireDashboardAuthentication.PolicyName);

        group.MapGet("/login", async Task<IResult> (string? t, string? returnUrl, HttpContext context) =>
        {
            var target = SafeReturnUrl(context, returnUrl, normalizedPrefix);
            if (t is not null)
            {
                if (SurefireDashboardAuthentication.TokensEqual(token, t))
                {
                    await SignInAsync(context, token);
                    return Results.Redirect(target);
                }

                LogRejectedSignIn(context);

                // Redirect so the bad token is stripped from the address bar.
                var query = returnUrl is null
                    ? "?error=1"
                    : $"?error=1&returnUrl={Uri.EscapeDataString(returnUrl)}";
                return Results.Redirect($"{context.Request.PathBase}{normalizedPrefix}/login{query}");
            }

            var auth = await context.AuthenticateAsync(SurefireDashboardAuthentication.AuthenticationScheme);
            if (auth.Succeeded)
            {
                return Results.Redirect(target);
            }

            context.Response.Headers.CacheControl = "no-store";
            return Results.Content(LoginPageHtml, "text/html; charset=utf-8");
        }).AllowAnonymous();

        group.MapPost("/login",
            async Task<Results<NoContent, ProblemHttpResult>> (LoginRequest request, HttpContext context) =>
            {
                if (request.Token is { } provided && SurefireDashboardAuthentication.TokensEqual(token, provided))
                {
                    await SignInAsync(context, token);
                    return TypedResults.NoContent();
                }

                LogRejectedSignIn(context);
                return TypedResults.Problem(
                    statusCode: StatusCodes.Status401Unauthorized,
                    title: "Unauthorized",
                    detail: "The token is not valid.");
            }).AllowAnonymous();
    }

    private static Task SignInAsync(HttpContext context, string token)
        => context.SignInAsync(
            SurefireDashboardAuthentication.AuthenticationScheme,
            SurefireDashboardAuthentication.CreatePrincipal(token),
            new AuthenticationProperties { IsPersistent = true });

    private static string? NonEmpty(string? value)
        => string.IsNullOrWhiteSpace(value) ? null : value;

    // Never log the attempted token value.
    private static void LogRejectedSignIn(HttpContext context)
        => context.RequestServices.GetRequiredService<ILoggerFactory>()
            .CreateLogger("Surefire.Dashboard")
            .LogWarning("A dashboard sign-in attempt was rejected: invalid token.");

    /// <summary>
    ///     Honors only same-origin absolute paths so the login endpoint cannot be used as an
    ///     open redirector.
    /// </summary>
    private static string SafeReturnUrl(HttpContext context, string? returnUrl, string normalizedPrefix)
    {
        // Same-origin absolute paths only, and no control characters: browsers strip tab/CR/LF,
        // which would turn "/\t/evil" into a protocol-relative redirect.
        if (returnUrl is "/" or ['/', not ('/' or '\\'), ..]
            && !returnUrl.AsSpan().ContainsAnyInRange('\0', '\u001f')
            && !returnUrl.Contains('\u007f'))
        {
            return returnUrl;
        }

        return $"{context.Request.PathBase}{normalizedPrefix}/";
    }

    private static void AddHostAuthorizationGuard(IEndpointRouteBuilder endpoints, RouteGroupBuilder group)
    {
        var services = endpoints.ServiceProvider;

        // Captured so the startup check can force endpoint building during StartAsync.
        services.GetRequiredService<DashboardAuthState>().DataSources = endpoints.DataSources;

        // Finally runs after caller-chained conventions, so this sees the final metadata.
        ((IEndpointConventionBuilder)group).Finally(builder =>
        {
            if (builder.Metadata.Any(static m => m is IAuthorizeData or IAllowAnonymous))
            {
                return;
            }

            var fallback = services.GetService<IOptions<AuthorizationOptions>>()?.Value.FallbackPolicy;
            if (fallback is null)
            {
                throw new InvalidOperationException(
                    "The Surefire dashboard is configured with DashboardAuthMode.HostAuthorization " +
                    "but nothing protects its endpoints. Chain .RequireAuthorization(...) on the " +
                    "builder returned by MapSurefireDashboard, configure a global FallbackPolicy, " +
                    "or opt out explicitly with options.AuthMode = DashboardAuthMode.Unsecured.");
            }
        });
    }

    private const string LoginPageHtml = """
        <!doctype html>
        <html lang="en">
        <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta name="color-scheme" content="light dark">
        <title>Sign in to Surefire</title>
        <style>
            :root { color-scheme: light dark; }
            body {
                margin: 0; min-height: 100dvh; display: grid; place-items: center;
                font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
                background: light-dark(#fafafa, #09090b); color: light-dark(#18181b, #fafafa);
            }
            main {
                width: min(92vw, 24rem); padding: 2rem; border-radius: 0.75rem;
                border: 1px solid light-dark(#e4e4e7, #27272a);
                background: light-dark(#ffffff, #18181b);
            }
            h1 { margin: 0 0 .25rem; font-size: 1.25rem; }
            p { margin: 0 0 1.25rem; font-size: .875rem; color: light-dark(#71717a, #a1a1aa); }
            input {
                width: 100%; box-sizing: border-box; padding: .5rem .75rem; font-size: .875rem;
                border-radius: .5rem; border: 1px solid light-dark(#e4e4e7, #3f3f46);
                background: transparent; color: inherit;
            }
            button {
                width: 100%; margin-top: .75rem; padding: .5rem .75rem; font-size: .875rem;
                border-radius: .5rem; border: none; cursor: pointer;
                background: light-dark(#18181b, #fafafa); color: light-dark(#fafafa, #18181b);
            }
            #err { display: none; margin: .75rem 0 0; font-size: .8125rem; color: #ef4444; }
        </style>
        </head>
        <body>
        <main>
            <h1>Surefire</h1>
            <p>Enter the dashboard token to sign in. Generated tokens are printed in the application logs at startup.</p>
            <form id="f">
                <input id="t" type="password" placeholder="Token" autocomplete="off" autofocus required>
                <button type="submit">Sign in</button>
                <p id="err">That token is not valid.</p>
            </form>
        </main>
        <script>
            const params = new URLSearchParams(location.search);
            const err = document.getElementById("err");
            if (params.get("error")) err.style.display = "block";
            // Trailing-slash requests also serve this page, so resolve the POST path absolutely.
            const loginPath = location.pathname.replace(/\/+$/, "");
            document.getElementById("f").addEventListener("submit", async (e) => {
                e.preventDefault();
                try {
                    const res = await fetch(loginPath, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ token: document.getElementById("t").value.trim() })
                    });
                    if (res.ok) {
                        const ret = params.get("returnUrl");
                        const ok = ret && ret.startsWith("/") && !ret.startsWith("//") && !ret.startsWith("/\\") && !/[\x00-\x1f\x7f]/.test(ret);
                        location.assign(ok ? ret : loginPath.slice(0, loginPath.lastIndexOf("/") + 1));
                        return;
                    }
                } catch (e) {
                    console.error(e);
                }
                err.style.display = "block";
            });
        </script>
        </body>
        </html>
        """;
}
