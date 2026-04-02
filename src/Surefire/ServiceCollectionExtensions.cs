using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Surefire;

namespace Microsoft.Extensions.DependencyInjection;

/// <summary>
///     Extension methods for registering Surefire services.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    ///     Adds Surefire core services with in-memory defaults for store and notifications.
    /// </summary>
    /// <param name="services">The service collection to configure.</param>
    /// <param name="configure">Optional callback for configuring <see cref="SurefireOptions" />.</param>
    /// <returns>The service collection for chaining.</returns>
    public static IServiceCollection AddSurefire(this IServiceCollection services,
        Action<SurefireOptions>? configure = null)
    {
        var options = new SurefireOptions();
        configure?.Invoke(options);
        options.Validate();

        services.TryAddSingleton(TimeProvider.System);
        services.AddMetrics();
        services.AddSingleton(options);
        services.TryAddSingleton<JobRegistry>();
        services.TryAddSingleton<ActiveRunTracker>();
        services.TryAddSingleton<SurefireInstrumentation>();
        services.TryAddSingleton<SurefireLogEventPump>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService>(
            sp => sp.GetRequiredService<SurefireLogEventPump>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, SurefireLoggerProvider>());
        services.AddHealthChecks().AddCheck<SurefireHealthCheck>("surefire");
        services.TryAddSingleton<IJobStore, InMemoryJobStore>();
        services.TryAddSingleton<INotificationProvider, InMemoryNotificationProvider>();
        services.TryAddSingleton<IJobClient, JobClient>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, SurefireInitializationService>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, SurefireExecutorService>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, SurefireSchedulerService>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, SurefireMaintenanceService>());
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, SurefireRetentionService>());

        foreach (var configureServices in options.ServiceConfigurators)
        {
            configureServices(services);
        }

        return services;
    }
}