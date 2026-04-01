namespace Surefire.Tests.Conformance;

public abstract class BatchConformanceTests : StoreConformanceBase
{
    private async Task<JobRun> CreateCoordinatorRunAsync(int batchTotal, JobStatus status = JobStatus.Running)
    {
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        run.BatchTotal = batchTotal;
        run.BatchCompleted = 0;
        run.BatchFailed = 0;
        run.NodeName = "batch";

        await Store.CreateRunsAsync([run]);

        if (status == JobStatus.Running)
        {
            run.Status = JobStatus.Running;
            run.Attempt = 1;
            run.StartedAt = DateTimeOffset.UtcNow;
            run.LastHeartbeatAt = DateTimeOffset.UtcNow;
            await Store.TryTransitionRunAsync(Transition(run, JobStatus.Pending));
        }

        return run;
    }

    [Fact]
    public async Task IncrementBatchCounter_Completed_IncrementsCompleted()
    {
        var run = await CreateCoordinatorRunAsync(5);

        var counters = await Store.TryIncrementBatchCounterAsync(run.Id, false);
        Assert.NotNull(counters);

        Assert.Equal(5, counters.Value.Total);
        Assert.Equal(1, counters.Value.Completed);
        Assert.Equal(0, counters.Value.Failed);
    }

    [Fact]
    public async Task IncrementBatchCounter_Failed_IncrementsBoth()
    {
        var run = await CreateCoordinatorRunAsync(5);

        var counters = await Store.TryIncrementBatchCounterAsync(run.Id, true);
        Assert.NotNull(counters);

        Assert.Equal(5, counters.Value.Total);
        Assert.Equal(0, counters.Value.Completed);
        Assert.Equal(1, counters.Value.Failed);
    }

    [Fact]
    public async Task IncrementBatchCounter_Returns_PostIncrementValues()
    {
        var run = await CreateCoordinatorRunAsync(10);

        var c1 = await Store.TryIncrementBatchCounterAsync(run.Id, false);
        Assert.NotNull(c1);
        Assert.Equal(1, c1.Value.Completed);

        var c2 = await Store.TryIncrementBatchCounterAsync(run.Id, false);
        Assert.NotNull(c2);
        Assert.Equal(2, c2.Value.Completed);

        var c3 = await Store.TryIncrementBatchCounterAsync(run.Id, true);
        Assert.NotNull(c3);
        Assert.Equal(2, c3.Value.Completed);
        Assert.Equal(1, c3.Value.Failed);
    }

    [Fact]
    public async Task IncrementBatchCounter_Concurrent_NoLostIncrements()
    {
        // Multiple trials help surface lost-increment races across store implementations.
        for (var trial = 0; trial < 20; trial++)
        {
            var run = await CreateCoordinatorRunAsync(100);

            var tasks = Enumerable.Range(0, 100)
                .Select(_ => Store.TryIncrementBatchCounterAsync(run.Id, false))
                .ToArray();

            await Task.WhenAll(tasks);

            var stored = await Store.GetRunAsync(run.Id);
            Assert.NotNull(stored);
            Assert.Equal(100, stored.BatchCompleted);
        }
    }

    [Fact]
    public async Task IncrementBatchCounter_CrossesThreshold_ExactlyOnce()
    {
        for (var trial = 0; trial < 20; trial++)
        {
            var run = await CreateCoordinatorRunAsync(10);

            var tasks = Enumerable.Range(0, 10)
                .Select(_ => Store.TryIncrementBatchCounterAsync(run.Id, false))
                .ToArray();

            var results = await Task.WhenAll(tasks);

            var crossedCount = results.Count(c => c is { } value && value.Completed + value.Failed == value.Total);
            Assert.Equal(1, crossedCount);
        }
    }

    [Fact]
    public async Task IncrementBatchCounter_AlreadyComplete_NoOp()
    {
        var jobName = $"BatchJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName, JobStatus.Running);
        run.BatchTotal = 5;
        run.BatchCompleted = 5;
        run.BatchFailed = 0;
        run.NodeName = "batch";
        run.Attempt = 1;
        run.StartedAt = DateTimeOffset.UtcNow;
        run.LastHeartbeatAt = DateTimeOffset.UtcNow;
        await Store.CreateRunsAsync([run]);

        run.Status = JobStatus.Completed;
        run.CompletedAt = DateTimeOffset.UtcNow;
        await Store.TryTransitionRunAsync(Transition(run, JobStatus.Running));

        var counters = await Store.TryIncrementBatchCounterAsync(run.Id, false);
        Assert.NotNull(counters);

        Assert.Equal(5, counters.Value.Total);
        Assert.Equal(5, counters.Value.Completed);
        Assert.Equal(0, counters.Value.Failed);
    }

    [Fact]
    public async Task IncrementBatchCounter_NonExistentRun_ReturnsNull()
    {
        var fakeId = Guid.CreateVersion7().ToString("N");

        var counters = await Store.TryIncrementBatchCounterAsync(fakeId, false);
        Assert.Null(counters);
    }

    [Fact]
    public async Task IncrementBatchCounter_NormalRun_ReturnsNull()
    {
        var jobName = $"NormalJob_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(jobName));

        var run = CreateRun(jobName);
        await Store.CreateRunsAsync([run]);

        var counters = await Store.TryIncrementBatchCounterAsync(run.Id, false);
        Assert.Null(counters);
    }
}