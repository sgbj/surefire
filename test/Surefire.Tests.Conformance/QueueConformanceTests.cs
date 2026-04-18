namespace Surefire.Tests.Conformance;

public abstract class QueueConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task UpsertQueue_CreatesAndUpdates()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"queue-{Guid.CreateVersion7():N}";

        await Store.UpsertQueueAsync(new() { Name = name, MaxConcurrency = 5, Priority = 1 }, ct);

        var queues = await Store.GetQueuesAsync(ct);
        var queue = Assert.Single(queues, q => q.Name == name);
        Assert.Equal(5, queue.MaxConcurrency);
        Assert.Equal(1, queue.Priority);
        Assert.False(queue.IsPaused);

        await Store.UpsertQueueAsync(new() { Name = name, MaxConcurrency = 10, Priority = 2 }, ct);

        queues = await Store.GetQueuesAsync(ct);
        queue = Assert.Single(queues, q => q.Name == name);
        Assert.Equal(10, queue.MaxConcurrency);
        Assert.Equal(2, queue.Priority);
    }

    [Fact]
    public async Task GetQueues_ReturnsAll()
    {
        var ct = TestContext.Current.CancellationToken;
        var names = Enumerable.Range(0, 3)
            .Select(_ => $"queue-{Guid.CreateVersion7():N}")
            .ToList();

        foreach (var name in names)
        {
            await Store.UpsertQueueAsync(new() { Name = name }, ct);
        }

        var queues = await Store.GetQueuesAsync(ct);

        foreach (var name in names)
        {
            Assert.Contains(queues, q => q.Name == name);
        }
    }

    [Fact]
    public async Task SetQueuePaused_TogglesState()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = name }, ct);

        await Store.SetQueuePausedAsync(name, true, ct);
        var queues = await Store.GetQueuesAsync(ct);
        Assert.True(Assert.Single(queues, q => q.Name == name).IsPaused);

        await Store.SetQueuePausedAsync(name, false, ct);
        queues = await Store.GetQueuesAsync(ct);
        Assert.False(Assert.Single(queues, q => q.Name == name).IsPaused);
    }

    [Fact]
    public async Task SetQueuePaused_SurvivesUpsert()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"queue-{Guid.CreateVersion7():N}";
        await Store.UpsertQueueAsync(new() { Name = name, Priority = 1 }, ct);

        await Store.SetQueuePausedAsync(name, true, ct);

        await Store.UpsertQueueAsync(new() { Name = name, Priority = 2 }, ct);

        var queues = await Store.GetQueuesAsync(ct);
        var queue = Assert.Single(queues, q => q.Name == name);
        Assert.True(queue.IsPaused);
        Assert.Equal(2, queue.Priority);
    }

    [Fact]
    public async Task SetQueuePaused_NonExistentQueue_NoOp()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"ghost-{Guid.CreateVersion7():N}";
        await Store.SetQueuePausedAsync(name, true, ct);
    }

    [Fact]
    public async Task UpsertQueue_AllFieldsRoundTrip()
    {
        var ct = TestContext.Current.CancellationToken;
        var queueName = $"QueueFields_{Guid.CreateVersion7():N}";
        var queue = new QueueDefinition
        {
            Name = queueName,
            Priority = 5,
            MaxConcurrency = 10,
            RateLimitName = "my-rate-limit"
        };
        await Store.UpsertQueueAsync(queue, ct);

        var queues = await Store.GetQueuesAsync(ct);
        var stored = queues.SingleOrDefault(q => q.Name == queueName);
        Assert.NotNull(stored);
        Assert.Equal(5, stored.Priority);
        Assert.Equal(10, stored.MaxConcurrency);
        Assert.Equal("my-rate-limit", stored.RateLimitName);
        Assert.NotNull(stored.LastHeartbeatAt);
    }

    [Fact]
    public async Task SetQueuePaused_CreatesImplicitQueue()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"implicit-{Guid.CreateVersion7():N}";

        // Queue doesn't exist yet — pause should create it
        await Store.SetQueuePausedAsync(name, true, ct);

        var queues = await Store.GetQueuesAsync(ct);
        var queue = Assert.Single(queues, q => q.Name == name);
        Assert.True(queue.IsPaused);
    }

    [Fact]
    public async Task SetQueuePaused_PreservesExistingSettings()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"explicit-{Guid.CreateVersion7():N}";

        await Store.UpsertQueueAsync(new()
        {
            Name = name,
            Priority = 7,
            MaxConcurrency = 20,
            RateLimitName = "rl"
        }, ct);

        await Store.SetQueuePausedAsync(name, true, ct);

        var queues = await Store.GetQueuesAsync(ct);
        var queue = Assert.Single(queues, q => q.Name == name);
        Assert.True(queue.IsPaused);
        Assert.Equal(7, queue.Priority);
        Assert.Equal(20, queue.MaxConcurrency);
        Assert.Equal("rl", queue.RateLimitName);
    }

    [Fact]
    public async Task SetQueuePaused_ResumeImplicitQueue()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"resume-{Guid.CreateVersion7():N}";

        await Store.SetQueuePausedAsync(name, true, ct);
        await Store.SetQueuePausedAsync(name, false, ct);

        var queues = await Store.GetQueuesAsync(ct);
        var queue = Assert.Single(queues, q => q.Name == name);
        Assert.False(queue.IsPaused);
    }

    [Fact]
    public async Task UpsertQueue_DoesNotOverwritePause()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"pause-preserve-{Guid.CreateVersion7():N}";

        // Pause via SetQueuePausedAsync
        await Store.SetQueuePausedAsync(name, true, ct);

        // Maintenance-style upsert should NOT overwrite is_paused
        await Store.UpsertQueueAsync(new() { Name = name, Priority = 3 }, ct);

        var queues = await Store.GetQueuesAsync(ct);
        var queue = Assert.Single(queues, q => q.Name == name);
        Assert.True(queue.IsPaused);
    }
}