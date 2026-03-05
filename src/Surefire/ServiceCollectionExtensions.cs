using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSurefire(this IServiceCollection services, Action<SurefireOptions>? configure = null)
    {
        var options = new SurefireOptions { Services = services };
        configure?.Invoke(options);
        options.Services = null!; // Not needed at runtime

        services.TryAddSingleton(options);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<JobRegistry>();
        services.TryAddSingleton<RunRecovery>();
        services.TryAddSingleton<JobExecutor>();
        services.TryAddSingleton<IJobStore, InMemoryJobStore>();
        services.TryAddSingleton<INotificationProvider, InMemoryNotificationProvider>();
        services.TryAddSingleton<IJobClient, JobClient>();
        services.TryAddSingleton<SurefireLoggerProvider>();
        services.TryAddEnumerable(ServiceDescriptor.Singleton<ILoggerProvider, SurefireLoggerProvider>(sp => sp.GetRequiredService<SurefireLoggerProvider>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SurefireMigrationService>());
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IHostedService, SurefireScheduler>());

        return services;
    }
}
