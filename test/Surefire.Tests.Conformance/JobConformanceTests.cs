namespace Surefire.Tests.Conformance;

public abstract class JobConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task UpsertJob_CreatesNew()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob();
        job.Description = "A test job";
        job.Tags = ["alpha", "beta"];
        job.CronExpression = "0 * * * *";
        job.MaxConcurrency = 5;
        job.Priority = 10;

        await Store.UpsertJobAsync(job, ct);

        var loaded = await Store.GetJobAsync(job.Name, ct);
        Assert.NotNull(loaded);
        Assert.Equal(job.Name, loaded.Name);
        Assert.Equal("A test job", loaded.Description);
        Assert.Equal(["alpha", "beta"], loaded.Tags);
        Assert.Equal("0 * * * *", loaded.CronExpression);
        Assert.Equal(5, loaded.MaxConcurrency);
        Assert.Equal(10, loaded.Priority);
        Assert.True(loaded.IsEnabled);
    }

    [Fact]
    public async Task UpsertJob_AllFieldsRoundTrip()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = new JobDefinition
        {
            Name = $"AllFields_{Guid.CreateVersion7():N}",
            Description = "Test desc",
            Tags = ["tag1", "tag2"],
            CronExpression = "0 * * * *",
            TimeZoneId = "America/Chicago",
            Timeout = TimeSpan.FromMinutes(5),
            MaxConcurrency = 3,
            Priority = 10,
            RetryPolicy = new() { MaxRetries = 5, BackoffType = BackoffType.Exponential },
            IsContinuous = true,
            Queue = "my-queue",
            RateLimitName = "my-limit",
            MisfirePolicy = MisfirePolicy.FireAll,
            FireAllLimit = 7,
            ArgumentsSchema = """{"type":"object"}"""
        };
        await Store.UpsertJobAsync(job, ct);

        var stored = await Store.GetJobAsync(job.Name, ct);
        Assert.NotNull(stored);
        Assert.Equal(job.Description, stored.Description);
        Assert.Equal(job.Tags, stored.Tags);
        Assert.Equal(job.CronExpression, stored.CronExpression);
        Assert.Equal(job.TimeZoneId, stored.TimeZoneId);
        Assert.Equal(job.Timeout, stored.Timeout);
        Assert.Equal(job.MaxConcurrency, stored.MaxConcurrency);
        Assert.Equal(job.Priority, stored.Priority);
        Assert.Equal(job.RetryPolicy.MaxRetries, stored.RetryPolicy.MaxRetries);
        Assert.Equal(job.RetryPolicy.BackoffType, stored.RetryPolicy.BackoffType);
        Assert.True(stored.IsContinuous);
        Assert.Equal(job.Queue, stored.Queue);
        Assert.Equal(job.RateLimitName, stored.RateLimitName);
        Assert.Equal(job.MisfirePolicy, stored.MisfirePolicy);
        Assert.Equal(job.FireAllLimit, stored.FireAllLimit);
        Assert.Equal(job.ArgumentsSchema, stored.ArgumentsSchema);
        Assert.True(stored.IsEnabled);
        Assert.NotNull(stored.LastHeartbeatAt);
    }

    [Fact]
    public async Task UpsertJob_TimeoutTicks_RoundTripsExactly()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob($"TimeoutTicks_{Guid.CreateVersion7():N}");
        job.Timeout = TimeSpan.FromTicks(12_345_679);

        await Store.UpsertJobAsync(job, ct);

        var stored = await Store.GetJobAsync(job.Name, ct);
        Assert.NotNull(stored);
        Assert.Equal(job.Timeout, stored.Timeout);
    }

    [Fact]
    public async Task UpsertJob_ClearsFireAllLimit_WhenSetToNull()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"ClearFireAllLimit_{Guid.CreateVersion7():N}";

        var initial = CreateJob(name);
        initial.MisfirePolicy = MisfirePolicy.FireAll;
        initial.FireAllLimit = 3;
        await Store.UpsertJobAsync(initial, ct);

        var updated = CreateJob(name);
        updated.MisfirePolicy = MisfirePolicy.FireAll;
        updated.FireAllLimit = null;
        await Store.UpsertJobAsync(updated, ct);

        var loaded = await Store.GetJobAsync(name, ct);
        Assert.NotNull(loaded);
        Assert.Null(loaded.FireAllLimit);
    }

    [Fact]
    public async Task UpsertJob_UpdatesExisting_PreservesIsEnabled()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob("PreserveEnabled");
        job.Description = "Original";
        await Store.UpsertJobAsync(job, ct);

        await Store.SetJobEnabledAsync("PreserveEnabled", false, ct);

        var updated = CreateJob("PreserveEnabled");
        updated.Description = "Updated";
        await Store.UpsertJobAsync(updated, ct);

        var loaded = await Store.GetJobAsync("PreserveEnabled", ct);
        Assert.NotNull(loaded);
        Assert.Equal("Updated", loaded.Description);
        Assert.False(loaded.IsEnabled);
    }

    [Fact]
    public async Task UpsertJob_PreservesLastCronFireAt()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob("PreserveCron");
        await Store.UpsertJobAsync(job, ct);

        var fireAt = new DateTimeOffset(2025, 6, 15, 12, 0, 0, TimeSpan.Zero);
        await Store.UpdateLastCronFireAtAsync("PreserveCron", fireAt, ct);

        var updated = CreateJob("PreserveCron");
        updated.Description = "Re-registered";
        await Store.UpsertJobAsync(updated, ct);

        var loaded = await Store.GetJobAsync("PreserveCron", ct);
        Assert.NotNull(loaded);
        Assert.Equal(fireAt, loaded.LastCronFireAt);
        Assert.Equal("Re-registered", loaded.Description);
    }

    [Fact]
    public async Task UpsertJob_InitialInsert_LastCronFireAtIsNull()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob($"NoCron_{Guid.CreateVersion7():N}");
        job.LastCronFireAt = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        await Store.UpsertJobAsync(job, ct);

        var loaded = await Store.GetJobAsync(job.Name, ct);
        Assert.NotNull(loaded);
        Assert.Null(loaded.LastCronFireAt);
    }

    [Fact]
    public async Task GetJob_ReturnsIsolatedCopy()
    {
        var ct = TestContext.Current.CancellationToken;
        var job = CreateJob($"Isolation_{Guid.CreateVersion7():N}");
        job.RetryPolicy = new() { MaxRetries = 3, BackoffType = BackoffType.Exponential };
        await Store.UpsertJobAsync(job, ct);

        var copy1 = await Store.GetJobAsync(job.Name, ct);
        Assert.NotNull(copy1);
        copy1.RetryPolicy = copy1.RetryPolicy with { MaxRetries = 99 };
        copy1.Description = "mutated";

        var copy2 = await Store.GetJobAsync(job.Name, ct);
        Assert.NotNull(copy2);
        Assert.Equal(3, copy2.RetryPolicy.MaxRetries);
        Assert.Null(copy2.Description);
    }

    [Fact]
    public async Task GetJob_ReturnsNull_WhenNotFound()
    {
        var ct = TestContext.Current.CancellationToken;
        var result = await Store.GetJobAsync("NonExistentJob_" + Guid.CreateVersion7().ToString("N"), ct);
        Assert.Null(result);
    }

    [Fact]
    public async Task GetJobs_FiltersByName()
    {
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobAsync(CreateJob($"OrderProcessor_{suffix}"), ct);
        await Store.UpsertJobAsync(CreateJob($"OrderNotifier_{suffix}"), ct);
        await Store.UpsertJobAsync(CreateJob($"InvoiceGenerator_{suffix}"), ct);

        var results = await Store.GetJobsAsync(new() { Name = "Order" }, ct);

        Assert.Contains(results, j => j.Name == $"OrderProcessor_{suffix}");
        Assert.Contains(results, j => j.Name == $"OrderNotifier_{suffix}");
        Assert.DoesNotContain(results, j => j.Name == $"InvoiceGenerator_{suffix}");
    }

    [Fact]
    public async Task GetJobs_FiltersByTag()
    {
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.CreateVersion7().ToString("N");

        var job1 = CreateJob($"TaggedA_{suffix}");
        job1.Tags = ["billing", "critical"];
        await Store.UpsertJobAsync(job1, ct);

        var job2 = CreateJob($"TaggedB_{suffix}");
        job2.Tags = ["shipping"];
        await Store.UpsertJobAsync(job2, ct);

        var job3 = CreateJob($"TaggedC_{suffix}");
        job3.Tags = ["billing"];
        await Store.UpsertJobAsync(job3, ct);

        var results = await Store.GetJobsAsync(new() { Tag = "billing" }, ct);

        Assert.Contains(results, j => j.Name == $"TaggedA_{suffix}");
        Assert.Contains(results, j => j.Name == $"TaggedC_{suffix}");
        Assert.DoesNotContain(results, j => j.Name == $"TaggedB_{suffix}");
    }

    [Fact]
    public async Task GetJobs_FiltersByEnabled()
    {
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.CreateVersion7().ToString("N");
        await Store.UpsertJobAsync(CreateJob($"Enabled_{suffix}"), ct);
        await Store.UpsertJobAsync(CreateJob($"Disabled_{suffix}"), ct);
        await Store.SetJobEnabledAsync($"Disabled_{suffix}", false, ct);

        var enabled = await Store.GetJobsAsync(new() { IsEnabled = true }, ct);
        var disabled = await Store.GetJobsAsync(new() { IsEnabled = false }, ct);

        Assert.Contains(enabled, j => j.Name == $"Enabled_{suffix}");
        Assert.DoesNotContain(enabled, j => j.Name == $"Disabled_{suffix}");
        Assert.Contains(disabled, j => j.Name == $"Disabled_{suffix}");
        Assert.DoesNotContain(disabled, j => j.Name == $"Enabled_{suffix}");
    }

    [Fact]
    public async Task SetJobEnabled_TogglesState()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"Toggle_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(name), ct);

        var before = await Store.GetJobAsync(name, ct);
        Assert.NotNull(before);
        Assert.True(before.IsEnabled);

        await Store.SetJobEnabledAsync(name, false, ct);
        var afterDisable = await Store.GetJobAsync(name, ct);
        Assert.NotNull(afterDisable);
        Assert.False(afterDisable.IsEnabled);

        await Store.SetJobEnabledAsync(name, true, ct);
        var afterEnable = await Store.GetJobAsync(name, ct);
        Assert.NotNull(afterEnable);
        Assert.True(afterEnable.IsEnabled);
    }

    [Fact]
    public async Task SetJobEnabled_NonExistentJob_NoOp()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"Ghost_{Guid.CreateVersion7():N}";
        await Store.SetJobEnabledAsync(name, false, ct);
    }

    [Fact]
    public async Task UpdateLastCronFireAt_Persists()
    {
        var ct = TestContext.Current.CancellationToken;
        var name = $"CronFire_{Guid.CreateVersion7():N}";
        await Store.UpsertJobAsync(CreateJob(name), ct);

        var fireAt = new DateTimeOffset(2025, 3, 1, 8, 0, 0, TimeSpan.Zero);
        await Store.UpdateLastCronFireAtAsync(name, fireAt, ct);

        var loaded = await Store.GetJobAsync(name, ct);
        Assert.NotNull(loaded);
        Assert.Equal(fireAt, loaded.LastCronFireAt);
    }

    [Fact]
    public async Task GetJobs_FilterByHeartbeatAfter()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"HBFilter_{Guid.CreateVersion7():N}";
        var job = CreateJob(jobName);
        await Store.UpsertJobAsync(job, ct);

        var stored = await Store.GetJobAsync(jobName, ct);
        Assert.NotNull(stored);

        var results = await Store.GetJobsAsync(new()
        {
            HeartbeatAfter = stored.LastHeartbeatAt!.Value.AddSeconds(-1)
        }, ct);
        Assert.Contains(results, j => j.Name == jobName);

        var noResults = await Store.GetJobsAsync(new()
        {
            HeartbeatAfter = stored.LastHeartbeatAt!.Value.AddMinutes(1)
        }, ct);
        Assert.DoesNotContain(noResults, j => j.Name == jobName);
    }

    [Fact]
    public async Task GetJobs_FiltersByTag_CaseInsensitive()
    {
        var ct = TestContext.Current.CancellationToken;
        var suffix = Guid.CreateVersion7().ToString("N");

        var job = CreateJob($"CaseTag_{suffix}");
        job.Tags = ["Billing"];
        await Store.UpsertJobAsync(job, ct);

        var results = await Store.GetJobsAsync(new() { Tag = "billing" }, ct);

        Assert.Contains(results, j => j.Name == $"CaseTag_{suffix}");
    }
}