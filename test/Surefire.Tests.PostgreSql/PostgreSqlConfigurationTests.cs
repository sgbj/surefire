using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Surefire.PostgreSql;

namespace Surefire.Tests.PostgreSql;

public sealed class PostgreSqlConfigurationTests
{
    [Fact]
    public async Task UsePostgreSql_ResolvesDataSourceFromDI_AndRegistersStoreAndNotifications()
    {
        var services = new ServiceCollection();
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=localhost;Username=test;Password=test;Database=test");
        services.AddSingleton(dataSource);

        services.AddSurefire(options => options.UsePostgreSql());

        await using var provider = services.BuildServiceProvider();

        _ = Assert.IsType<PostgreSqlJobStore>(provider.GetRequiredService<IJobStore>());
        _ = Assert.IsType<PostgreSqlNotificationProvider>(provider.GetRequiredService<INotificationProvider>());
    }

    [Fact]
    public async Task UsePostgreSql_WithFactory_RegistersStoreAndNotifications()
    {
        var services = new ServiceCollection();
        await using var dataSource = NpgsqlDataSource.Create(
            "Host=localhost;Username=test;Password=test;Database=test");

        services.AddSurefire(options => options.UsePostgreSql(_ => dataSource));

        await using var provider = services.BuildServiceProvider();

        _ = Assert.IsType<PostgreSqlJobStore>(provider.GetRequiredService<IJobStore>());
        _ = Assert.IsType<PostgreSqlNotificationProvider>(provider.GetRequiredService<INotificationProvider>());
    }
}