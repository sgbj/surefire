using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Surefire.Tests.Fakes;

namespace Surefire.Tests;

public sealed class SchedulerServiceTests
{
    private static (SurefireSchedulerService service, FakeSchedulerStore store, FakeTimeProvider time)
        CreateService(SurefireOptions? options = null)
    {
        var time = new FakeTimeProvider();
        time.SetUtcNow(new(2025, 1, 1, 12, 0, 30, TimeSpan.Zero));
        var store = new FakeSchedulerStore();
        var notifications = new FakeNotificationProvider();
        var opts = options ?? new SurefireOptions();
        var service = new SurefireSchedulerService(
            store, notifications, opts, time,
            new(new DummyMeterFactory()),
            new(time),
            new(() => 0),
            NullLogger<SurefireSchedulerService>.Instance);
        return (service, store, time);
    }

    private static JobDefinition MakeJob(
        string name = "TestJob",
        string cron = "* * * * *",
        MisfirePolicy policy = MisfirePolicy.FireOnce,
        int? fireAllLimit = null,
        DateTimeOffset? lastCronFireAt = null,
        string? timeZoneId = null,
        bool isEnabled = true) =>
        new()
        {
            Name = name,
            CronExpression = cron,
            MisfirePolicy = policy,
            FireAllLimit = fireAllLimit,
            LastCronFireAt = lastCronFireAt,
            TimeZoneId = timeZoneId,
            IsEnabled = isEnabled,
            Priority = 0
        };

    [Fact]
    public async Task FirstFire_SetsLastCronFireAt_NoRunCreated()
    {
        var (service, store, time) = CreateService();
        store.Jobs.Add(MakeJob(lastCronFireAt: null));
        var now = time.GetUtcNow();

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Equal(now, store.LastCronFireAtUpdates["TestJob"]);
        Assert.Empty(store.CreatedRuns);
    }

    [Fact]
    public async Task Skip_UpdatesLastCronFireAt_NoRunCreated()
    {
        var (service, store, time) = CreateService();
        var lastFire = time.GetUtcNow().AddMinutes(-3);
        store.Jobs.Add(MakeJob(cron: "* * * * *", policy: MisfirePolicy.Skip, lastCronFireAt: lastFire));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Empty(store.CreatedRuns);
        Assert.True(store.LastCronFireAtUpdates.ContainsKey("TestJob"));
        // Updated to most recent missed fire, not now
        Assert.True(store.LastCronFireAtUpdates["TestJob"] < time.GetUtcNow());
        Assert.True(store.LastCronFireAtUpdates["TestJob"] > lastFire);
    }

    [Fact]
    public async Task FireOnce_MultipleMissed_CreatesExactlyOneRun()
    {
        var (service, store, time) = CreateService();
        var lastFire = time.GetUtcNow().AddMinutes(-5);
        store.Jobs.Add(MakeJob(cron: "* * * * *", policy: MisfirePolicy.FireOnce, lastCronFireAt: lastFire));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Single(store.CreatedRuns);
    }

    [Fact]
    public async Task FireOnce_UsesLatestMissedFireTimeForDeduplication()
    {
        var (service, store, time) = CreateService();
        var lastFire = time.GetUtcNow().AddMinutes(-2).AddSeconds(-30);
        store.Jobs.Add(MakeJob(cron: "* * * * *", policy: MisfirePolicy.FireOnce, lastCronFireAt: lastFire));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Single(store.CreatedRuns);
        var (run, cronFireAt) = store.CreatedRuns[0];
        Assert.Contains("cron:TestJob:", run.DeduplicationId);
        // The cron fire at passed to the store should be the most recent missed fire
        Assert.True(cronFireAt > lastFire);
    }

    [Fact]
    public async Task FireAll_MultipleMissed_CreatesOneRunPerFireTime()
    {
        var (service, store, time) = CreateService();
        var lastFire = time.GetUtcNow().AddMinutes(-3).AddSeconds(-30);
        store.Jobs.Add(MakeJob(cron: "* * * * *", policy: MisfirePolicy.FireAll, lastCronFireAt: lastFire));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Equal(3, store.CreatedRuns.Count);
    }

    [Fact]
    public async Task FireAll_ExceedsLimit_FiresNewestNAndCursorLandsOnLatestMiss()
    {
        var (service, store, time) = CreateService();
        var now = time.GetUtcNow();
        var lastFire = now.AddMinutes(-10).AddSeconds(-30);
        store.Jobs.Add(MakeJob(cron: "* * * * *", policy: MisfirePolicy.FireAll,
            fireAllLimit: 3, lastCronFireAt: lastFire));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        // 10 misses available (one per minute over 10:30), limit 3 keeps the newest 3.
        Assert.Equal(
            [
                now.AddMinutes(-2).AddSeconds(-30),
                now.AddMinutes(-1).AddSeconds(-30),
                now.AddSeconds(-30)
            ],
            store.CreatedRuns.Select(x => x.CronFireAt).ToArray());

        // Cursor lands on the latest miss so subsequent ticks don't replay the older skipped fires.
        Assert.Equal(now.AddSeconds(-30), store.CreatedRuns[^1].CronFireAt);
    }

    [Fact]
    public async Task FireAll_RespectsTimeZoneAcrossSpringForward()
    {
        var (service, store, time) = CreateService();
        time.SetUtcNow(new(2025, 3, 10, 14, 30, 0, TimeSpan.Zero));

        store.Jobs.Add(MakeJob(
            cron: "0 9 * * *",
            policy: MisfirePolicy.FireAll,
            lastCronFireAt: new DateTimeOffset(2025, 3, 8, 15, 0, 0, TimeSpan.Zero),
            timeZoneId: "America/Chicago"));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Equal(
            [
                new(2025, 3, 9, 14, 0, 0, TimeSpan.Zero),
                new(2025, 3, 10, 14, 0, 0, TimeSpan.Zero)
            ],
            store.CreatedRuns.Select(x => x.CronFireAt).ToArray());
    }

    [Fact]
    public async Task FireAll_RespectsTimeZoneAcrossFallBack()
    {
        var (service, store, time) = CreateService();
        time.SetUtcNow(new(2025, 11, 3, 15, 30, 0, TimeSpan.Zero));

        store.Jobs.Add(MakeJob(
            cron: "0 9 * * *",
            policy: MisfirePolicy.FireAll,
            lastCronFireAt: new DateTimeOffset(2025, 11, 1, 14, 0, 0, TimeSpan.Zero),
            timeZoneId: "America/Chicago"));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Equal(
            [
                new(2025, 11, 2, 15, 0, 0, TimeSpan.Zero),
                new(2025, 11, 3, 15, 0, 0, TimeSpan.Zero)
            ],
            store.CreatedRuns.Select(x => x.CronFireAt).ToArray());
    }

    [Fact]
    public async Task InvalidTimeZone_SkipsJob_NoRunCreated()
    {
        var (service, store, time) = CreateService();
        time.SetUtcNow(new(2025, 1, 1, 12, 30, 0, TimeSpan.Zero));

        store.Jobs.Add(MakeJob(
            cron: "0 9 * * *",
            policy: MisfirePolicy.FireAll,
            lastCronFireAt: new DateTimeOffset(2024, 12, 31, 9, 0, 0, TimeSpan.Zero),
            timeZoneId: "Invalid/Zone"));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Empty(store.CreatedRuns);
        Assert.Empty(store.LastCronFireAtUpdates);
    }

    [Fact]
    public async Task InvalidCron_SkipsJob_NoException()
    {
        var (service, store, time) = CreateService();
        store.Jobs.Add(MakeJob(cron: "not a cron", lastCronFireAt: time.GetUtcNow().AddMinutes(-1)));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Empty(store.CreatedRuns);
        Assert.Empty(store.LastCronFireAtUpdates);
    }

    [Fact]
    public async Task NoMissedFires_NoRunCreated()
    {
        var (service, store, time) = CreateService();
        // lastCronFireAt is 10s ago on a per-minute cron, so next fire is not due yet.
        store.Jobs.Add(MakeJob(cron: "* * * * *", policy: MisfirePolicy.FireOnce,
            lastCronFireAt: time.GetUtcNow().AddSeconds(-10)));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Empty(store.CreatedRuns);
        Assert.Empty(store.LastCronFireAtUpdates);
    }

    [Fact]
    public async Task Deduplication_SameFireTime_OnlyOneRunCreated()
    {
        var (service, store, time) = CreateService();
        var lastFire = time.GetUtcNow().AddMinutes(-1).AddSeconds(-30);
        store.Jobs.Add(MakeJob(cron: "* * * * *", policy: MisfirePolicy.FireOnce, lastCronFireAt: lastFire));

        await service.ScheduleDueJobsAsync(CancellationToken.None);
        Assert.Equal(1, store.TryCreateCallCount);

        // Time hasn't advanced; the store rejects the duplicate via dedup.
        store.RejectNextCreate = true;
        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Equal(2, store.TryCreateCallCount);
        Assert.Single(store.CreatedRuns);
    }

    [Fact]
    public async Task DisabledJob_NotScheduled()
    {
        var (service, store, time) = CreateService();
        // Disabled jobs are filtered out by GetJobsAsync(IsEnabled = true)
        store.Jobs.Add(MakeJob(isEnabled: false, lastCronFireAt: time.GetUtcNow().AddMinutes(-1)));

        await service.ScheduleDueJobsAsync(CancellationToken.None);

        Assert.Empty(store.CreatedRuns);
    }

    [Fact]
    public async Task SchedulerLoop_NonTransientStoreError_LoopContinuesAndRecordsFailure()
    {
        // The scheduler loop must not rethrow out of ExecuteAsync. A non-transient error
        // must be logged and counted, and the next tick must keep firing.
        var time = new FakeTimeProvider();
        time.SetUtcNow(new(2025, 1, 1, 12, 0, 0, TimeSpan.Zero));
        var store = new ExplodingSchedulerStore();
        var notifications = new FakeNotificationProvider();
        var loopHealth = new LoopHealthTracker(time);
        var service = new SurefireSchedulerService(
            store, notifications, new(), time,
            new(new DummyMeterFactory()),
            loopHealth,
            new(() => 0),
            NullLogger<SurefireSchedulerService>.Instance);

        using var cts = new CancellationTokenSource();
        var execute = service.StartAsync(cts.Token);

        // Let the scheduler poll ~4 times (default PollingInterval is 5s).
        for (var i = 0; i < 4; i++)
        {
            time.Advance(TimeSpan.FromSeconds(5));
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }

        await cts.CancelAsync();
        await service.StopAsync(CancellationToken.None);
        await execute;

        var state = loopHealth.Snapshot()[SurefireSchedulerService.LoopName];
        Assert.True(state.ConsecutiveFailures > 0,
            $"Expected at least one recorded failure, got {state.ConsecutiveFailures}");
        Assert.True(store.Calls >= 2, $"Loop should survive to retry; Calls={store.Calls}");
    }

    private sealed class FakeNotificationProvider : INotificationProvider
    {
        public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task PublishAsync(string channel, string? message = null,
            CancellationToken cancellationToken = default) =>
            Task.CompletedTask;

        public Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string?, Task> handler,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IAsyncDisposable>(NoopDisposable.Instance);

        private sealed class NoopDisposable : IAsyncDisposable
        {
            public static readonly NoopDisposable Instance = new();
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    private sealed class ExplodingSchedulerStore : ThrowingJobStore
    {
        private int _calls;
        public int Calls => Volatile.Read(ref _calls);

        public override Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
            CancellationToken ct = default)
        {
            Interlocked.Increment(ref _calls);
            throw new InvalidOperationException("non-transient failure");
        }

        public override bool IsTransientException(Exception ex) => false;
    }

    internal sealed class FakeSchedulerStore : ThrowingJobStore
    {
        public List<JobDefinition> Jobs { get; } = [];
        public List<(JobRun Run, DateTimeOffset CronFireAt)> CreatedRuns { get; } = [];
        public Dictionary<string, DateTimeOffset> LastCronFireAtUpdates { get; } = [];
        public int TryCreateCallCount { get; private set; }
        public bool RejectNextCreate { get; set; }

        public override Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null,
            CancellationToken ct = default)
        {
            IReadOnlyList<JobDefinition> result = filter?.IsEnabled is true
                ? Jobs.Where(j => j.IsEnabled).ToList()
                : Jobs;
            return Task.FromResult(result);
        }

        public override Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null,
            DateTimeOffset? lastCronFireAt = null,
            IReadOnlyList<RunEvent>? initialEvents = null,
            CancellationToken ct = default)
        {
            TryCreateCallCount++;
            if (RejectNextCreate)
            {
                RejectNextCreate = false;
                return Task.FromResult(false);
            }

            CreatedRuns.Add((run, lastCronFireAt ?? default));
            return Task.FromResult(true);
        }

        public override Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt,
            CancellationToken ct = default)
        {
            LastCronFireAtUpdates[jobName] = fireAt;
            return Task.CompletedTask;
        }
    }

    private sealed class DummyMeterFactory : IMeterFactory
    {
        private readonly Meter _meter = new("test");
        public Meter Create(MeterOptions options) => _meter;
        public void Dispose() => _meter.Dispose();
    }
}
