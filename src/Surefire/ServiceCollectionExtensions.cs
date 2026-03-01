using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace Surefire;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSurefire(this IServiceCollection services, Action<SurefireOptions>? configure = null)
    {
        var options = new SurefireOptions();
        configure?.Invoke(options);

        services.TryAddSingleton(options);
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<JobRegistry>();
        services.TryAddSingleton<JobExecutor>();
        services.TryAddSingleton<IJobStore, InMemoryJobStore>();
        services.TryAddSingleton<INotificationProvider, InMemoryNotificationProvider>();
        services.TryAddSingleton<IJobClient, JobClient>();
        services.TryAddSingleton<SurefireLoggerProvider>();
        services.AddSingleton<ILoggerProvider>(sp => sp.GetRequiredService<SurefireLoggerProvider>());
        services.AddHostedService<SurefireMigrationService>();
        services.AddHostedService<SurefireScheduler>();

        return services;
    }
}
