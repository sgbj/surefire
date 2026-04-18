using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Surefire.Redis;

namespace Surefire.Tests.Redis;

public sealed class RedisConfigurationTests
{
    [Fact]
    public async Task UseRedis_ResolvesMultiplexerFromDI_AndRegistersStoreAndNotifications()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IConnectionMultiplexer>(_ =>
            ConnectionMultiplexer.Connect(new ConfigurationOptions
            {
                EndPoints = { "localhost:6379" },
                AbortOnConnectFail = false,
                ConnectTimeout = 1
            }));

        services.AddSurefire(options => options.UseRedis());

        await using var provider = services.BuildServiceProvider();

        _ = Assert.IsType<RedisJobStore>(provider.GetRequiredService<IJobStore>());
        _ = Assert.IsType<RedisNotificationProvider>(provider.GetRequiredService<INotificationProvider>());
    }

    [Fact]
    public async Task UseRedis_WithFactory_RegistersStoreAndNotifications()
    {
        var services = new ServiceCollection();
        var mux = ConnectionMultiplexer.Connect(new ConfigurationOptions
        {
            EndPoints = { "localhost:6379" },
            AbortOnConnectFail = false,
            ConnectTimeout = 1
        });

        services.AddSurefire(options => options.UseRedis(_ => mux));

        await using var provider = services.BuildServiceProvider();

        _ = Assert.IsType<RedisJobStore>(provider.GetRequiredService<IJobStore>());
        _ = Assert.IsType<RedisNotificationProvider>(provider.GetRequiredService<INotificationProvider>());
    }
}