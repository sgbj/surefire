using Microsoft.Extensions.DependencyInjection;
using Surefire.SqlServer;

namespace Surefire.Tests.SqlServer;

public sealed class SqlServerConfigurationTests
{
    private const string TestConnectionString =
        "Server=(localdb)\\MSSQLLocalDB;Integrated Security=true;TrustServerCertificate=true";

    [Fact]
    public void UseSqlServer_RegistersStore_WithConfiguredTimeout()
    {
        var services = new ServiceCollection();

        services.AddSurefire(options =>
            options.UseSqlServer(TestConnectionString, TimeSpan.FromSeconds(37)));

        using var provider = services.BuildServiceProvider();

        var store = Assert.IsType<SqlServerJobStore>(provider.GetRequiredService<IJobStore>());
        Assert.Equal(37, store.CommandTimeoutSeconds);
    }
}