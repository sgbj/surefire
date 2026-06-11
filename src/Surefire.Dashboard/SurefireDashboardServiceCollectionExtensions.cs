using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Surefire.Dashboard;

namespace Microsoft.AspNetCore.Builder;

/// <summary>
///     DI extensions that prepare the host for <c>MapSurefireDashboard</c>.
/// </summary>
public static class SurefireDashboardServiceCollectionExtensions
{
    /// <summary>
    ///     Registers the services and JSON metadata required by <c>MapSurefireDashboard</c>. Call
    ///     once during DI configuration.
    /// </summary>
    /// <param name="services">The DI container.</param>
    /// <param name="configure">Optional callback to override <see cref="SurefireDashboardOptions" /> defaults.</param>
    public static IServiceCollection AddSurefireDashboard(this IServiceCollection services,
        Action<SurefireDashboardOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.ConfigureHttpJsonOptions(static options =>
        {
            var chain = options.SerializerOptions.TypeInfoResolverChain;
            if (!chain.Contains(SurefireDashboardJsonContext.Default))
            {
                chain.Insert(0, SurefireDashboardJsonContext.Default);
            }
        });

        var options = new SurefireDashboardOptions();
        configure?.Invoke(options);
        services.AddSingleton(options);
        services.AddSingleton<DashboardAuthState>();

        if (options.AuthMode == DashboardAuthMode.BrowserToken)
        {
            services.AddAuthentication()
                .AddCookie(SurefireDashboardAuthentication.AuthenticationScheme, static cookie =>
                {
                    cookie.Cookie.Name = ".Surefire.Dashboard";
                    cookie.Cookie.HttpOnly = true;
                    cookie.Cookie.SameSite = SameSiteMode.Lax;
                    cookie.Cookie.SecurePolicy = CookieSecurePolicy.SameAsRequest;
                    cookie.ExpireTimeSpan = TimeSpan.FromDays(3);
                    cookie.SlidingExpiration = true;
                    cookie.Events = new CookieAuthenticationEvents
                    {
                        // Cookies are bound to the token they were minted for, so rotating the
                        // token signs every session out.
                        OnValidatePrincipal = static async context =>
                        {
                            var state = context.HttpContext.RequestServices
                                .GetRequiredService<DashboardAuthState>();
                            var claim = context.Principal?
                                .FindFirst(SurefireDashboardAuthentication.TokenClaimType)?.Value;
                            if (state.Token is not { } token
                                || claim is null
                                || !SurefireDashboardAuthentication.TokensEqual(
                                    SurefireDashboardAuthentication.Fingerprint(token), claim))
                            {
                                context.HttpContext.RequestServices
                                    .GetRequiredService<ILoggerFactory>()
                                    .CreateLogger("Surefire.Dashboard")
                                    .LogWarning(
                                        "A dashboard cookie was rejected because it was issued for a different browser token. " +
                                        "If you run multiple replicas, set one token (Surefire:Dashboard:BrowserToken) and share a Data Protection key ring.");
                                context.RejectPrincipal();
                                await context.HttpContext.SignOutAsync(
                                    SurefireDashboardAuthentication.AuthenticationScheme);
                            }
                        },
                        // API calls get bare 401s; browser navigation goes to the login page.
                        OnRedirectToLogin = static context =>
                        {
                            var state = context.HttpContext.RequestServices
                                .GetRequiredService<DashboardAuthState>();
                            if (state.Prefix is not { } prefix)
                            {
                                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                                return Task.CompletedTask;
                            }

                            if (context.Request.Path.StartsWithSegments($"{prefix}/api"))
                            {
                                context.Response.StatusCode = StatusCodes.Status401Unauthorized;
                            }
                            else
                            {
                                var returnUrl = $"{context.Request.PathBase}{context.Request.Path}{context.Request.QueryString}";
                                context.Response.Redirect(
                                    $"{context.Request.PathBase}{prefix}/login?returnUrl={Uri.EscapeDataString(returnUrl)}");
                            }

                            return Task.CompletedTask;
                        },
                        OnRedirectToAccessDenied = static context =>
                        {
                            context.Response.StatusCode = StatusCodes.Status403Forbidden;
                            return Task.CompletedTask;
                        }
                    };
                });

            services.AddAuthorization(static authorization =>
                authorization.AddPolicy(SurefireDashboardAuthentication.PolicyName, static policy => policy
                    .AddAuthenticationSchemes(SurefireDashboardAuthentication.AuthenticationScheme)
                    .RequireClaim(SurefireDashboardAuthentication.TokenClaimType)));

            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, DashboardLoginUrlLogger>());
        }
        else if (options.AuthMode == DashboardAuthMode.HostAuthorization)
        {
            services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, DashboardAuthStartupCheck>());
        }

        return services;
    }
}
