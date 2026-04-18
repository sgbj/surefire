using Microsoft.Extensions.Logging.Abstractions;

namespace Surefire.Tests;

public sealed class InMemoryNotificationProviderTests
{
    [Fact]
    public async Task PublishAsync_WaitsForSubscriberHandlers()
    {
        var ct = TestContext.Current.CancellationToken;

        var provider = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var enteredHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var subscription = await provider.SubscribeAsync(
            NotificationChannels.RunCreated,
            async _ =>
            {
                enteredHandler.TrySetResult();
                await releaseHandler.Task;
            },
            ct);

        var publishTask = provider.PublishAsync(NotificationChannels.RunCreated, "run-1", ct);

        await enteredHandler.Task.WaitAsync(TimeSpan.FromSeconds(1), ct);
        Assert.False(publishTask.IsCompleted);

        releaseHandler.TrySetResult();
        await publishTask.WaitAsync(TimeSpan.FromSeconds(1), ct);
        Assert.True(publishTask.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task DisposingOneSubscription_LeavesOtherSubscriptionsOnSameChannelIntact()
    {
        var ct = TestContext.Current.CancellationToken;

        // Regression: Redis previously called UnsubscribeAsync(channel) without the handler arg,
        // which removes EVERY handler for the channel, not just the one being disposed.
        var provider = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);

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
        Assert.Equal(1, Volatile.Read(ref aFired));
        Assert.Equal(1, Volatile.Read(ref bFired));

        await subA.DisposeAsync();

        await provider.PublishAsync(NotificationChannels.RunCreated, "run-2", ct);
        Assert.Equal(1, Volatile.Read(ref aFired)); // unchanged — A was disposed
        Assert.Equal(2, Volatile.Read(ref bFired)); // still receiving — B is intact
    }
}