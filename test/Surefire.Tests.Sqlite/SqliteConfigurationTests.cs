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
            options.UseSqlite(new SqliteOptions
            {
                ConnectionString = "Data Source=:memory:",
                CommandTimeout = TimeSpan.FromSeconds(25)
            }));

        using var provider = services.BuildServiceProvider();

        var store = Assert.IsType<SqliteJobStore>(provider.GetRequiredService<IJobStore>());
        Assert.Equal(25, store.CommandTimeoutSeconds);
    }
}
