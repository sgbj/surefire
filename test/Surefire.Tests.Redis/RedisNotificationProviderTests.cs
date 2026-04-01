using Microsoft.Extensions.Logging.Abstractions;
using Surefire.Redis;

namespace Surefire.Tests.Redis;

public sealed class RedisNotificationProviderTests
{
    [Fact]
    public async Task PublishAsync_BeforeInitialize_ThrowsInvalidOperationException()
    {
        await using var provider = new RedisNotificationProvider(
            new RedisOptions("localhost:1,abortConnect=false,connectTimeout=100"),
            NullLogger<RedisNotificationProvider>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.PublishAsync(NotificationChannels.RunCreated, "run-1"));
    }
}
