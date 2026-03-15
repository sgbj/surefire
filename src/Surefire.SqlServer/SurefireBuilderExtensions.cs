using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Surefire.SqlServer;

public static class SurefireBuilderExtensions
{
    public static SurefireBuilder UseSqlServer(this SurefireBuilder builder, string connectionString, Action<SqlServerOptions>? configure = null)
    {
        var options = new SqlServerOptions { ConnectionString = connectionString };
        configure?.Invoke(options);
        builder.Services.Replace(ServiceDescriptor.Singleton(_ => options));
        builder.Services.Replace(ServiceDescriptor.Singleton<IJobStore, SqlServerJobStore>());
        return builder;
    }
}
