using Microsoft.Extensions.DependencyInjection;
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

        return services;
    }
}
