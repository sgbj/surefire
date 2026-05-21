using System.Diagnostics.Metrics;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Surefire.Tests.Fakes;
using Surefire.Tests.Testing;

namespace Surefire.Tests;

public sealed class MaintenanceServiceTests
{
    [Fact]
    public async Task RunMaintenanceTick_ExpiredRunSignalsActiveWorkCascadesDescendantsAndAppendsNeutralFailure()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = new FakeTimeProvider(new(2025, 6, 15, 10, 0, 0, TimeSpan.Zero));
        var root = new JobRun
        {
            Id = "root-run",
            JobName = "job",
            Status = JobStatus.Canceled,
            CreatedAt = time.GetUtcNow().AddMinutes(-10),
            NotBefore = time.GetUtcNow().AddMinutes(-10),
            CompletedAt = time.GetUtcNow(),
            CanceledAt = time.GetUtcNow(),
            Reason = "Run expired past its deadline.",
            Attempt = 3,
            LeaseEpoch = 1
        };

        var child = new CanceledRun("child-run", "batch-1");
        var childReason = "Canceled because parent run 'root-run' expired.";
        var store = new ExpirationStore
        {
            ExpiredResult = new SubtreeCancellation(
                [new(root.Id, root.BatchId), child],
                [new("batch-1", JobStatus.Canceled, time.GetUtcNow())])
            {
                ExpiredRuns =
                [
                    new(root.Id, root.BatchId, root.Attempt, root.Reason!, ExpiredCancellationKind.Expired),
                    new(child.RunId, child.BatchId, root.Attempt, childReason, ExpiredCancellationKind.AncestorExpired)
                ]
            }
        };
        store.Runs[root.Id] = root;
        store.Runs[child.RunId] = root with
        {
            Id = child.RunId,
            BatchId = child.BatchId,
            Reason = childReason
        };

        var notifications = new CapturingNotificationProvider();
        var activeRuns = new ActiveRunTracker();
        using var rootCts = new CancellationTokenSource();
        using var childCts = new CancellationTokenSource();
        activeRuns.Add(root.Id, rootCts);
        activeRuns.Add(child.RunId, childCts);

        await using var eventWriter = await TestEventWriter.StartAsync(store, notifications);
        var batchCompletion = new BatchCompletionHandler(
            store,
            notifications,
            eventWriter,
            time,
            NullLogger<BatchCompletionHandler>.Instance);
        var service = new SurefireMaintenanceService(
            store,
            notifications,
            new(),
            activeRuns,
            new(),
            time,
            new(new DummyMeterFactory()),
            new(time),
            batchCompletion,
            new(store, notifications, activeRuns),
            new(() => 0.5),
            NullLogger<SurefireMaintenanceService>.Instance);

        await service.RunMaintenanceTickAsync(ct);
        await eventWriter.Writer.FlushRunAsync(root.Id, ct);

        Assert.True(rootCts.IsCancellationRequested);
        Assert.True(childCts.IsCancellationRequested);
        Assert.Contains(notifications.Publications,
            p => p.Channel == NotificationChannels.RunCancel(root.Id) && p.Message is null);
        Assert.Contains(notifications.Publications,
            p => p.Channel == NotificationChannels.RunCancel(child.RunId) && p.Message is null);
        Assert.Contains(notifications.Publications,
            p => p.Channel == NotificationChannels.BatchTerminated("batch-1") && p.Message == "batch-1");

        Assert.Empty(store.CascadeCalls);
        Assert.Equal(0, store.GetRunCalls);

        var failureEvents = store.AppendedEvents.Where(e => e.EventType == RunEventType.AttemptFailure).ToList();
        Assert.Equal(2, failureEvents.Count);
        var envelope = JsonSerializer.Deserialize(
            failureEvents.Single(e => e.RunId == root.Id).Payload,
            SurefireJsonContext.Default.RunFailureEnvelope);
        Assert.NotNull(envelope);
        Assert.Equal("expired_cancellation", envelope.FailureCode);
        Assert.Equal("Canceled: run expired past its deadline.", envelope.Message);

        var childEnvelope = JsonSerializer.Deserialize(
            failureEvents.Single(e => e.RunId == child.RunId).Payload,
            SurefireJsonContext.Default.RunFailureEnvelope);
        Assert.NotNull(childEnvelope);
        Assert.Equal("parent_canceled", childEnvelope.FailureCode);
    }

    [Fact]
    public async Task InMemoryClaim_AllowsRunsExactlyAtExpirationBoundary()
    {
        var ct = TestContext.Current.CancellationToken;
        var time = new FakeTimeProvider(new(2025, 6, 15, 10, 0, 0, TimeSpan.Zero));
        var store = new InMemoryJobStore(time);
        var jobName = "boundary-job";
        await store.UpsertJobsAsync([new() { Name = jobName }], ct);

        var run = new JobRun
        {
            Id = "boundary-run",
            JobName = jobName,
            Status = JobStatus.Pending,
            CreatedAt = time.GetUtcNow(),
            NotBefore = time.GetUtcNow(),
            NotAfter = time.GetUtcNow(),
            ExpiresAt = time.GetUtcNow(),
            Attempt = 1
        };
        await store.CreateRunsAsync([run], cancellationToken: ct);

        var claimed = await store.ClaimRunsAsync("node", [jobName], ["default"], 1, ct);

        var claimedRun = Assert.Single(claimed);
        Assert.Equal(run.Id, claimedRun.Id);
    }

    private sealed class ExpirationStore : ThrowingJobStore
    {
        public Dictionary<string, JobRun> Runs { get; } = new(StringComparer.Ordinal);
        public List<RunEvent> AppendedEvents { get; } = [];
        public List<(string RootRunId, string? Reason, bool IncludeRoot)> CascadeCalls { get; } = [];
        public int GetRunCalls { get; private set; }
        public SubtreeCancellation ExpiredResult { get; init; } = SubtreeCancellation.Empty;
        public SubtreeCancellation CascadeResult { get; init; } = SubtreeCancellation.Empty;

        public override Task UpsertJobsAsync(IReadOnlyList<JobDefinition> jobs, CancellationToken ct = default) =>
            Task.CompletedTask;

        public override Task UpsertQueuesAsync(IReadOnlyList<QueueDefinition> queues, CancellationToken ct = default) =>
            Task.CompletedTask;

        public override Task UpsertRateLimitsAsync(IReadOnlyList<RateLimitDefinition> rateLimits,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public override Task HeartbeatAsync(string nodeName, IReadOnlyCollection<string> jobNames,
            IReadOnlyCollection<string> queueNames, IReadOnlyCollection<string> activeRunIds,
            CancellationToken ct = default) =>
            Task.CompletedTask;

        public override Task<IReadOnlyList<string>> GetStaleRunningRunIdsAsync(DateTimeOffset staleBefore, int take,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<string>>([]);

        public override Task<SubtreeCancellation> CancelExpiredRunsWithIdsAsync(
            CancellationToken cancellationToken = default) =>
            Task.FromResult(ExpiredResult);

        public override Task<JobRun?> GetRunAsync(string id, CancellationToken ct = default)
        {
            GetRunCalls++;
            Runs.TryGetValue(id, out var run);
            return Task.FromResult(run);
        }

        public override Task<SubtreeCancellation> CancelRunSubtreeAsync(string rootRunId, string? reason = null,
            bool includeRoot = true, CancellationToken cancellationToken = default)
        {
            CascadeCalls.Add((rootRunId, reason, includeRoot));
            return Task.FromResult(CascadeResult);
        }

        public override Task<IReadOnlyList<string>> GetCompletableBatchIdsAsync(
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IReadOnlyList<string>>([]);

        public override Task AppendEventsAsync(IReadOnlyList<RunEvent> events, CancellationToken ct = default)
        {
            AppendedEvents.AddRange(events);
            return Task.CompletedTask;
        }
    }

    private sealed class CapturingNotificationProvider : INotificationProvider
    {
        public List<(string Channel, string? Message)> Publications { get; } = [];

        public Task InitializeAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task PublishAsync(string channel, string? message = null,
            CancellationToken cancellationToken = default)
        {
            Publications.Add((channel, message));
            return Task.CompletedTask;
        }

        public Task<IAsyncDisposable> SubscribeAsync(string channel, Func<string?, Task> handler,
            CancellationToken cancellationToken = default) =>
            Task.FromResult<IAsyncDisposable>(NoopDisposable.Instance);

        private sealed class NoopDisposable : IAsyncDisposable
        {
            public static readonly NoopDisposable Instance = new();
            public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        }
    }

    private sealed class DummyMeterFactory : IMeterFactory
    {
        private readonly Meter _meter = new("test");
        public Meter Create(MeterOptions options) => _meter;
        public void Dispose() => _meter.Dispose();
    }
}
