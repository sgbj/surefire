using Microsoft.Extensions.DependencyInjection;
using Surefire.PostgreSql;

namespace Surefire.Tests.PostgreSql;

public sealed class PostgreSqlConfigurationTests
{
    [Fact]
    public async Task UsePostgreSql_RegistersStoreAndNotifications_WithConfiguredTimeout()
    {
        var services = new ServiceCollection();

        services.AddSurefire(options =>
            options.UsePostgreSql(new PostgreSqlOptions
            {
                ConnectionString = "Host=localhost;Username=test;Password=test;Database=test",
                CommandTimeout = TimeSpan.FromSeconds(42)
            }));

        await using var provider = services.BuildServiceProvider();

        var store = Assert.IsType<PostgreSqlJobStore>(provider.GetRequiredService<IJobStore>());
        _ = Assert.IsType<PostgreSqlNotificationProvider>(provider.GetRequiredService<INotificationProvider>());
        Assert.Equal(42, store.CommandTimeoutSeconds);
    }
}
