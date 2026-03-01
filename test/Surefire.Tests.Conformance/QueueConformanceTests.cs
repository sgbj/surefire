namespace Surefire.Tests.Conformance;

public abstract class QueueConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task UpsertQueue_CreatesAndUpdates()
    {
        var name = $"queue-{Guid.CreateVersion7():N}";

        await Store.UpsertQueueAsync(new() { Name = name, MaxConcurrency = 5, Priority = 1 });

        var queues = await Store.GetQueuesAsync();
        var queue = Assert.Single(queues, q => q.Name == name);
        Assert.Equal(5, queue.MaxConcurrency);
        Assert.Equal(1, queue.Priority);
        Assert.False(queue.IsPaused);

        await Store.UpsertQueueAsync(new() { Name = name, MaxConcurrency = 10, Priority = 2 });

        queues = await Store.GetQueuesAsync();
        queue = Assert.Single(queues, q => q.Name == name);
        Assert.Equal(10, queue.MaxConcurrency);
        Assert.Equal(2, queue.Priority);
    }

    [Fact]
    public async Task GetQueues_ReturnsAll()
    {
        var names = Enumerable.Range(0, 3)
            .Select(_ => $"queue-{Guid.CreateVersion7():N}")
            .ToList();

        foreach (var name in names)
        {
            await Store.UpsertQueueAsync(new() { Name = name });
        }

        var queues = await Store.GetQueuesAsync();

        foreach (var name in names)
        {
            Assert.Contains(queues, q => q.Name == name);
        }
    }

    [Fact]
    public async Task SetQueuePaused_TogglesState()
    {
        var name = $"queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = name });

        await Store.SetQueuePausedAsync(name, true);
        var queues = await Store.GetQueuesAsync();
        Assert.True(Assert.Single(queues, q => q.Name == name).IsPaused);

        await Store.SetQueuePausedAsync(name, false);
        queues = await Store.GetQueuesAsync();
        Assert.False(Assert.Single(queues, q => q.Name == name).IsPaused);
    }

    [Fact]
    public async Task SetQueuePaused_SurvivesUpsert()
    {
        var name = $"queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = name, Priority = 1 });

        await Store.SetQueuePausedAsync(name, true);

        await Store.UpsertQueueAsync(new() { Name = name, Priority = 2 });

        var queues = await Store.GetQueuesAsync();
        var queue = Assert.Single(queues, q => q.Name == name);
        Assert.True(queue.IsPaused);
        Assert.Equal(2, queue.Priority);
    }

    [Fact]
    public async Task SetQueuePaused_NonExistentQueue_NoOp()
    {
        var name = $"ghost-{Guid.CreateVersion7():N}";
        await Store.SetQueuePausedAsync(name, true);
    }

    [Fact]
    public async Task UpsertQueue_AllFieldsRoundTrip()
    {
        var queueName = $"QueueFields_{Guid.CreateVersion7():N}";
        var queue = new QueueDefinition
        {
            Name = queueName,
            Priority = 5,
            MaxConcurrency = 10,
            RateLimitName = "my-rate-limit"
        };
        await Store.UpsertQueueAsync(queue);

        var queues = await Store.GetQueuesAsync();
        var stored = queues.SingleOrDefault(q => q.Name == queueName);
        Assert.NotNull(stored);
        Assert.Equal(5, stored.Priority);
        Assert.Equal(10, stored.MaxConcurrency);
        Assert.Equal("my-rate-limit", stored.RateLimitName);
        Assert.NotNull(stored.LastHeartbeatAt);
    }
}