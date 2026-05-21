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

    [Fact]
    public async Task WithSqlCancellation_Normalizes_InvalidOperationCancellation()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var ex = await Assert.ThrowsAsync<OperationCanceledException>(() =>
            Task.FromException(new InvalidOperationException("Operation cancelled by user."))
                .WithSqlCancellation(cts.Token));

        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Equal(cts.Token, ex.CancellationToken);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Task.FromException(new InvalidOperationException("Operation cancelled by user."))
                .WithSqlCancellation(CancellationToken.None));
    }
}
