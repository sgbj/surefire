using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace Surefire.Tests;

public sealed class RetentionServiceTests
{
    private static (SurefireRetentionService service, FakePurgeStore store, FakeTimeProvider time)
        CreateService(SurefireOptions options)
    {
        var time = new FakeTimeProvider();
        time.SetUtcNow(new DateTimeOffset(2025, 6, 15, 10, 0, 0, TimeSpan.Zero));
        var store = new FakePurgeStore();
        var service = new SurefireRetentionService(
            store, options, time, NullLogger<SurefireRetentionService>.Instance);
        return (service, store, time);
    }

    [Fact]
    public async Task RetentionPeriodSet_PurgesWithCorrectThreshold()
    {
        var retention = TimeSpan.FromDays(7);
        var (service, store, time) = CreateService(new SurefireOptions { RetentionPeriod = retention });
        var expectedThreshold = time.GetUtcNow() - retention;

        await service.RunRetentionAsync(CancellationToken.None);

        Assert.Single(store.PurgeThresholds);
        Assert.Equal(expectedThreshold, store.PurgeThresholds[0]);
    }

    [Fact]
    public async Task RetentionPeriodNull_PurgeNotCalled()
    {
        var (service, store, _) = CreateService(new SurefireOptions { RetentionPeriod = null });

        await service.RunRetentionAsync(CancellationToken.None);

        Assert.Empty(store.PurgeThresholds);
    }

    [Fact]
    public async Task MultipleTicks_PurgesEachTime()
    {
        var (service, store, time) = CreateService(new SurefireOptions { RetentionPeriod = TimeSpan.FromHours(1) });

        await service.RunRetentionAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromMinutes(5));
        await service.RunRetentionAsync(CancellationToken.None);

        Assert.Equal(2, store.PurgeThresholds.Count);
        // Second threshold is 5 minutes later than the first
        Assert.True(store.PurgeThresholds[1] > store.PurgeThresholds[0]);
    }

    private sealed class FakePurgeStore : IJobStore
    {
        public List<DateTimeOffset> PurgeThresholds { get; } = [];

        public Task PurgeAsync(DateTimeOffset threshold, CancellationToken ct = default)
        {
            PurgeThresholds.Add(threshold);
            return Task.CompletedTask;
        }

        public Task MigrateAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task<JobDefinition?> GetJobAsync(string name, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<JobDefinition>> GetJobsAsync(JobListFilter? filter = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task UpsertJobAsync(JobDefinition job, CancellationToken ct = default) => throw new NotImplementedException();
        public Task SetJobEnabledAsync(string name, bool enabled, CancellationToken ct = default) => throw new NotImplementedException();
        public Task UpdateLastCronFireAtAsync(string jobName, DateTimeOffset fireAt, CancellationToken ct = default) => throw new NotImplementedException();
        public Task CreateRunsAsync(IReadOnlyList<JobRun> runs, IReadOnlyList<RunEvent>? initialEvents = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<bool> TryCreateRunAsync(JobRun run, int? maxActiveForJob = null, DateTimeOffset? lastCronFireAt = null, IReadOnlyList<RunEvent>? initialEvents = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<JobRun?> GetRunAsync(string id, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<PagedResult<JobRun>> GetRunsAsync(RunFilter filter, int skip = 0, int take = 50, CancellationToken ct = default) => throw new NotImplementedException();
        public Task UpdateRunAsync(JobRun run, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<bool> TryTransitionRunAsync(RunStatusTransition transition, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<bool> TryCancelRunAsync(string runId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<JobRun?> ClaimRunAsync(string nodeName, IReadOnlyCollection<string> jobNames, IReadOnlyCollection<string> queueNames, CancellationToken ct = default) => throw new NotImplementedException();
        public Task CreateBatchAsync(JobBatch batch, IReadOnlyList<JobRun> runs, IReadOnlyList<RunEvent>? initialEvents = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<BatchCounters?> TryIncrementBatchProgressAsync(string batchId, JobStatus terminalStatus, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<bool> TryCompleteBatchAsync(string batchId, JobStatus status, DateTimeOffset completedAt, CancellationToken ct = default) => throw new NotImplementedException();
        public Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<RunEvent>> GetEventsAsync(string runId, long sinceId = 0, RunEventType[]? types = null, int? attempt = null, int? take = null, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<RunEvent>> GetBatchOutputEventsAsync(string batchRunId, long sinceEventId = 0, int take = 200, CancellationToken ct = default) => throw new NotImplementedException();
        public Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames, IReadOnlyCollection<string> queueNames, IReadOnlyCollection<string> activeRunIds, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<NodeInfo>> GetNodesAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task UpsertQueueAsync(QueueDefinition queue, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyList<QueueDefinition>> GetQueuesAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task SetQueuePausedAsync(string name, bool isPaused, CancellationToken ct = default) => throw new NotImplementedException();
        public Task UpsertRateLimitAsync(RateLimitDefinition rateLimit, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<int> CancelExpiredRunsAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task<DashboardStats> GetDashboardStatsAsync(DateTimeOffset? since = null, int bucketMinutes = 60, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<JobStats> GetJobStatsAsync(string jobName, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<IReadOnlyDictionary<string, QueueStats>> GetQueueStatsAsync(CancellationToken ct = default) => throw new NotImplementedException();
    }
}
