using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Surefire.Redis;
using Testcontainers.Redis;

namespace Surefire.Tests.Redis;

public sealed class RedisNotificationProviderTests
{
    [Fact]
    public async Task PublishAsync_BeforeInitialize_ThrowsInvalidOperationException()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var connection = await ConnectionMultiplexer.ConnectAsync(
            "localhost:1,abortConnect=false,connectTimeout=100");
        var provider = new RedisNotificationProvider(
            connection,
            TimeProvider.System,
            NullLogger<RedisNotificationProvider>.Instance);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.PublishAsync(NotificationChannels.RunCreated, "run-1", ct));
    }

    [Fact]
    public async Task DisposingOneSubscription_LeavesOtherSubscriptionsOnSameChannelIntact()
    {
        // Regression: Subscription.DisposeAsync previously called UnsubscribeAsync(channel) without
        // the handler arg, which removes EVERY handler for the channel — silently breaking the
        // dashboard SSE + executor RunCancel + JobClient.WaitAsync co-tenancy on the same run id.
        var ct = TestContext.Current.CancellationToken;
        await using var container = new RedisBuilder().WithImage("redis:7-alpine").Build();
        await container.StartAsync(ct);

        await using var connection = await ConnectionMultiplexer.ConnectAsync(container.GetConnectionString());
        var provider = new RedisNotificationProvider(
            connection,
            TimeProvider.System,
            NullLogger<RedisNotificationProvider>.Instance);
        await provider.InitializeAsync(ct);

        var aFired = 0;
        var bFired = 0;

        var subA = await provider.SubscribeAsync(NotificationChannels.RunCreated, _ =>
        {
            Interlocked.Increment(ref aFired);
            return Task.CompletedTask;
        }, ct);
        await using var subB = await provider.SubscribeAsync(NotificationChannels.RunCreated, _ =>
        {
            Interlocked.Increment(ref bFired);
            return Task.CompletedTask;
        }, ct);

        await provider.PublishAsync(NotificationChannels.RunCreated, "run-1", ct);
        await WaitForCountAsync(() => Volatile.Read(ref aFired), 1, ct);
        await WaitForCountAsync(() => Volatile.Read(ref bFired), 1, ct);

        await subA.DisposeAsync();

        await provider.PublishAsync(NotificationChannels.RunCreated, "run-2", ct);
        await WaitForCountAsync(() => Volatile.Read(ref bFired), 2, ct);
        Assert.Equal(1, Volatile.Read(ref aFired)); // unchanged — A was disposed
    }

    private static async Task WaitForCountAsync(Func<int> read, int expected, CancellationToken cancellationToken)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(TimeSpan.FromSeconds(5));
        while (read() < expected)
        {
            cts.Token.ThrowIfCancellationRequested();
            await Task.Delay(10, cts.Token);
        }
    }
}