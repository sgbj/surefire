using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSurefire(this IServiceCollection services, Action<SurefireBuilder>? configure = null)
    {
        var builder = new SurefireBuilder(services);
        configure?.Invoke(builder);

        AddCoreServices(services, builder.Options);

        services.TryAddSingleton<RunRecovery>();
        services.TryAddSingleton<JobExecutor>();
        services.TryAddSingleton<SurefireLoggerProvider>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, SurefireLoggerProvider>(sp => sp.GetRequiredService<SurefireLoggerProvider>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SurefireMigrationService>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SurefireScheduler>());

        return services;
    }

    public static IServiceCollection AddSurefireClient(this IServiceCollection services, Action<SurefireBuilder>? configure = null)
    {
        var builder = new SurefireBuilder(services);
        configure?.Invoke(builder);

        AddCoreServices(services, builder.Options);

        return services;
    }

    private static void AddCoreServices(IServiceCollection services, SurefireOptions options)
    {
        services.TryAddSingleton(options);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<JobRegistry>();
        services.TryAddSingleton<IJobStore, InMemoryJobStore>();
        services.TryAddSingleton<INotificationProvider, InMemoryNotificationProvider>();
        services.TryAddSingleton<PlanEngine>();
        services.TryAddSingleton<IJobClient, JobClient>();
    }
}
