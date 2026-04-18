using Microsoft.Extensions.DependencyInjection;
using Surefire.Sqlite;

namespace Surefire.Tests.Sqlite;

public sealed class SqliteConfigurationTests
{
    [Fact]
    public void UseSqlite_RegistersStore_WithConfiguredTimeout()
    {
        var services = new ServiceCollection();

        services.AddSurefire(options =>
            options.UseSqlite("Data Source=:memory:", TimeSpan.FromSeconds(25)));

        using var provider = services.BuildServiceProvider();

        var store = Assert.IsType<SqliteJobStore>(provider.GetRequiredService<IJobStore>());
        Assert.Equal(25, store.CommandTimeoutSeconds);
    }

    [Fact]
    public void UseSqlite_WithFactory_RegistersStore()
    {
        var services = new ServiceCollection();

        services.AddSurefire(options =>
            options.UseSqlite(_ => "Data Source=:memory:"));

        using var provider = services.BuildServiceProvider();

        _ = Assert.IsType<SqliteJobStore>(provider.GetRequiredService<IJobStore>());
    }
}