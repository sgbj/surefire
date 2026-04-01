namespace Surefire.Tests;

public sealed class InMemoryNotificationProviderTests
{
    [Fact]
    public async Task PublishAsync_WaitsForSubscriberHandlers()
    {
        var provider = new InMemoryNotificationProvider();
        var enteredHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseHandler = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var subscription = await provider.SubscribeAsync(
            NotificationChannels.RunCreated,
            async _ =>
            {
                enteredHandler.TrySetResult();
                await releaseHandler.Task;
            });

        var publishTask = provider.PublishAsync(NotificationChannels.RunCreated, "run-1");

        await enteredHandler.Task.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.False(publishTask.IsCompleted);

        releaseHandler.TrySetResult();
        await publishTask.WaitAsync(TimeSpan.FromSeconds(1));
        Assert.True(publishTask.IsCompletedSuccessfully);
    }
}
