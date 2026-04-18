using Microsoft.Extensions.DependencyInjection;
using Surefire.SqlServer;

namespace Surefire.Tests.SqlServer;

public sealed class SqlServerConfigurationTests
{
    [Fact]
    public void UseSqlServer_RegistersStore_WithConfiguredTimeout()
    {
        var services = new ServiceCollection();

        services.AddSurefire(options =>
            options.UseSqlServer(
                "Server=(localdb)\\MSSQLLocalDB;Integrated Security=true;TrustServerCertificate=true",
                TimeSpan.FromSeconds(37)));

        using var provider = services.BuildServiceProvider();

        var store = Assert.IsType<SqlServerJobStore>(provider.GetRequiredService<IJobStore>());
        Assert.Equal(37, store.CommandTimeoutSeconds);
    }

    [Fact]
    public void UseSqlServer_WithFactory_RegistersStore()
    {
        var services = new ServiceCollection();

        services.AddSurefire(options =>
            options.UseSqlServer(_ =>
                "Server=(localdb)\\MSSQLLocalDB;Integrated Security=true;TrustServerCertificate=true"));

        using var provider = services.BuildServiceProvider();

        _ = Assert.IsType<SqlServerJobStore>(provider.GetRequiredService<IJobStore>());
    }
}