using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Time.Testing;

namespace Surefire.Tests;

public sealed class SurefireHealthCheckTests
{
    [Fact]
    public async Task CheckHealthAsync_ReturnsHealthy_WhenStoreNotificationsAndNodeAreHealthy()
    {
        var timeProvider = new FakeTimeProvider();
        timeProvider.SetUtcNow(new(2026, 4, 11, 3, 0, 0, TimeSpan.Zero));

        var options = new SurefireOptions
        {
            NodeName = "node-a",
            HeartbeatInterval = TimeSpan.FromSeconds(10),
            InactiveThreshold = TimeSpan.FromSeconds(30)
        };

        var store = new InMemoryJobStore(timeProvider);
        await store.HeartbeatAsync(options.NodeName, [], ["default"], [], CancellationToken.None);

        var check = new SurefireHealthCheck(
            store,
            new HealthyNotificationProvider(),
            options,
            timeProvider,
            new(timeProvider));

        var result = await check.CheckHealthAsync(new(), CancellationToken.None);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task CheckHealthAsync_ReturnsDegraded_WhenNotificationHealthProbeFails()
    {
        var timeProvider = new FakeTimeProvider();
        timeProvider.SetUtcNow(new(2026, 4, 11, 3, 0, 0, TimeSpan.Zero));

        var options = new SurefireOptions
        {
            NodeName = "node-a",
            HeartbeatInterval = TimeSpan.FromSeconds(10),
            InactiveThreshold = TimeSpan.FromSeconds(30)
        };

        var store = new InMemoryJobStore(timeProvider);
        await store.HeartbeatAsync(options.NodeName, [], ["default"], [], CancellationToken.None);

        var check = new SurefireHealthCheck(
            store,
            new FailingNotificationProvider(),
            options,
            timeProvider,
            new(timeProvider));

        var result = await check.CheckHealthAsync(new(), CancellationToken.None);

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("notification health probe failed", result.Description, StringComparison.OrdinalIgnoreCase);
    }

    private class HealthyNotificationProvider : INotificationProvider
    {
        public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
        public virtual Task PingAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task PublishAsync(string channel, string? message = null,
            CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string?, Task> handler,
            CancellationToken cancellationToken = default)
            => Task.FromResult<IAsyncDisposable>(NoopDisposable.Instance);
    }

    private sealed class FailingNotificationProvider : HealthyNotificationProvider
    {
        public override Task PingAsync(CancellationToken cancellationToken = default)
            => Task.FromException(new InvalidOperationException("notification backend unavailable"));
    }

    private sealed class NoopDisposable : IAsyncDisposable
    {
        public static readonly NoopDisposable Instance = new();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
