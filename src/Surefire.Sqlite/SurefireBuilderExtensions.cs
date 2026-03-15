using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Surefire.Sqlite;

public static class SurefireBuilderExtensions
{
    public static SurefireBuilder UseSqlite(this SurefireBuilder builder, string connectionString, Action<SqliteOptions>? configure = null)
    {
        var options = new SqliteOptions { ConnectionString = connectionString };
        configure?.Invoke(options);
        builder.Services.Replace(ServiceDescriptor.Singleton(_ => options));
        builder.Services.Replace(ServiceDescriptor.Singleton<IJobStore, SqliteJobStore>());
        return builder;
    }
}
