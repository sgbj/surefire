using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Surefire.PostgreSql;

public static class SurefireOptionsExtensions
{
    public static SurefireOptions UsePostgreSql(this SurefireOptions options, string connectionString, Action<PostgreSqlOptions>? configure = null)
    {
        var pgOptions = new PostgreSqlOptions { ConnectionString = connectionString };
        configure?.Invoke(pgOptions);
        options.Services.TryAddSingleton(_ => pgOptions);
        options.Services.AddSingleton<IJobStore, PostgreSqlJobStore>();
        options.Services.AddSingleton<INotificationProvider, PostgreSqlNotificationProvider>();
        return options;
    }

    public static SurefireOptions UsePostgreSqlStore(this SurefireOptions options, string connectionString, Action<PostgreSqlOptions>? configure = null)
    {
        var pgOptions = new PostgreSqlOptions { ConnectionString = connectionString };
        configure?.Invoke(pgOptions);
        options.Services.TryAddSingleton(_ => pgOptions);
        options.Services.AddSingleton<IJobStore, PostgreSqlJobStore>();
        return options;
    }

    public static SurefireOptions UsePostgreSqlNotifications(this SurefireOptions options, string connectionString, Action<PostgreSqlOptions>? configure = null)
    {
        var pgOptions = new PostgreSqlOptions { ConnectionString = connectionString };
        configure?.Invoke(pgOptions);
        options.Services.TryAddSingleton(_ => pgOptions);
        options.Services.AddSingleton<INotificationProvider, PostgreSqlNotificationProvider>();
        return options;
    }
}
