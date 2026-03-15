using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Surefire.PostgreSql;

public static class SurefireBuilderExtensions
{
    public static SurefireBuilder UsePostgreSql(this SurefireBuilder builder, string connectionString, Action<PostgreSqlOptions>? configure = null)
    {
        var pgOptions = new PostgreSqlOptions { ConnectionString = connectionString };
        configure?.Invoke(pgOptions);
        builder.Services.Replace(ServiceDescriptor.Singleton(_ => pgOptions));
        builder.Services.Replace(ServiceDescriptor.Singleton<IJobStore, PostgreSqlJobStore>());
        builder.Services.Replace(ServiceDescriptor.Singleton<INotificationProvider, PostgreSqlNotificationProvider>());
        return builder;
    }

    public static SurefireBuilder UsePostgreSqlStore(this SurefireBuilder builder, string connectionString, Action<PostgreSqlOptions>? configure = null)
    {
        var pgOptions = new PostgreSqlOptions { ConnectionString = connectionString };
        configure?.Invoke(pgOptions);
        builder.Services.Replace(ServiceDescriptor.Singleton(_ => pgOptions));
        builder.Services.Replace(ServiceDescriptor.Singleton<IJobStore, PostgreSqlJobStore>());
        return builder;
    }

    public static SurefireBuilder UsePostgreSqlNotifications(this SurefireBuilder builder, string connectionString, Action<PostgreSqlOptions>? configure = null)
    {
        var pgOptions = new PostgreSqlOptions { ConnectionString = connectionString };
        configure?.Invoke(pgOptions);
        builder.Services.Replace(ServiceDescriptor.Singleton(_ => pgOptions));
        builder.Services.Replace(ServiceDescriptor.Singleton<INotificationProvider, PostgreSqlNotificationProvider>());
        return builder;
    }
}
