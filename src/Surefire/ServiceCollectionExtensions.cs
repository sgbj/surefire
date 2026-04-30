using System.Diagnostics.CodeAnalysis;
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
    /// <remarks>
    ///     Lifecycle callbacks registered on <see cref="SurefireOptions" /> are reflected over and
    ///     compiled at registration time. A planned source generator will remove this requirement.
    /// </remarks>
    [RequiresUnreferencedCode("Compiles user-supplied lifecycle callback delegates.")]
    [RequiresDynamicCode("Compiles user-supplied lifecycle callback delegates.")]
    public static IServiceCollection AddSurefire(this IServiceCollection services,
        Action<SurefireOptions>? configure = null)
    {
        var options = new SurefireOptions();
        configure?.Invoke(options);
        options.Validate();
        var frozenOptions = options.Freeze();

        services.TryAddSingleton(TimeProvider.System);
        services.AddLogging();
        services.AddMetrics();
        services.TryAddSingleton(frozenOptions);
        services.TryAddSingleton<JobRegistry>();
        services.TryAddSingleton<ActiveRunTracker>();
        services.TryAddSingleton<SurefireInstrumentation>();
        services.TryAddSingleton<LoopHealthTracker>();
        services.TryAddSingleton<SurefireLogEventPump>();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, SurefireLogEventPump>(sp =>
                sp.GetRequiredService<SurefireLogEventPump>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, SurefireLoggerProvider>());
        services.AddHealthChecks().AddCheck<SurefireHealthCheck>("surefire");
        services.TryAddSingleton<Backoff>();
        services.TryAddSingleton<IJobStore, InMemoryJobStore>();
        services.TryAddSingleton<INotificationProvider, InMemoryNotificationProvider>();
        services.TryAddSingleton<IJobClient, JobClient>();
        services.TryAddSingleton<BatchCompletionHandler>();
        services.TryAddSingleton<RunCancellationCoordinator>();
        // Not registered as IHostedService: lifecycle is owned by SurefireExecutorService so
        // drain is strictly ordered around active runs. Hosted-service registration would race
        // under HostOptions.ServicesStopConcurrently = true and risk lost enqueues at shutdown.
        services.TryAddSingleton<BatchedEventWriter>();
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
