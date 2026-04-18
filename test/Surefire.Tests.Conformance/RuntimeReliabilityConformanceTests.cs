using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Surefire.Tests.Testing;

namespace Surefire.Tests.Conformance;

public abstract class RuntimeReliabilityConformanceTests : StoreConformanceBase
{
    [Fact]
    public async Task ContinuousJob_SeedsToMaxConcurrency_OnStartup()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ContinuousSeed_{Guid.CreateVersion7():N}";

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMilliseconds(20);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.InactiveThreshold = TimeSpan.FromMilliseconds(250);
                options.RetentionPeriod = null;
                options.MaxNodeConcurrency = 10;
            },
            (host, _) =>
            {
                host.AddJob(jobName, async (CancellationToken ct) =>
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return 1;
                }).Continuous().WithMaxConcurrency(3);
            });

        await harness.StartAsync(ct);

        var page = await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = jobName,
                ExactJobName = true,
                IsTerminal = false
            }, 0, 20),
            p => p.Items.Count == 3,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for startup continuous seeding to create max-concurrency runs.",
            ct);

        Assert.Equal(3, page.Items.Count);
    }

    [Fact]
    public async Task StaleRunningRun_IsRecoveredAndReclaimed_ByMaintenanceLoop()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"CrashRecovery_{Guid.CreateVersion7():N}";

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMilliseconds(20);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
                options.RetentionPeriod = null;
            },
            (host, _) => host.AddJob(jobName, () => 7).WithRetry(policy =>
            {
                policy.MaxRetries = 1;
                policy.InitialDelay = TimeSpan.FromMilliseconds(50);
            }));

        await harness.StartAsync(ct);

        var staleAt = DateTimeOffset.UtcNow.AddSeconds(-10);
        var stale = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Running,
            CreatedAt = staleAt,
            NotBefore = staleAt,
            StartedAt = staleAt,
            LastHeartbeatAt = staleAt,
            NodeName = "dead-node",
            Attempt = 1,
            Progress = 0
        };

        await harness.Store.CreateRunsAsync([stale], cancellationToken: ct);

        var recovered = await TestWait.AwaitRunAsync(
            harness.Store, harness.Notifications, stale.Id,
            run => run.Status == JobStatus.Succeeded,
            TimeSpan.FromSeconds(8),
            "Timed out waiting for stale running run to be recovered and completed.",
            ct);
        Assert.Equal(2, recovered.Attempt);
    }

    [Fact]
    public async Task StaleRunningRun_ExhaustedRetryPolicy_TransitionsToDeadLetter()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"CrashRecoveryNoRetry_{Guid.CreateVersion7():N}";

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMilliseconds(20);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
                options.RetentionPeriod = null;
            },
            (host, _) => host.AddJob(jobName, () => 7).WithRetry(0));

        await harness.StartAsync(ct);

        var staleAt = DateTimeOffset.UtcNow.AddSeconds(-10);
        var stale = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Status = JobStatus.Running,
            CreatedAt = staleAt,
            NotBefore = staleAt,
            StartedAt = staleAt,
            LastHeartbeatAt = staleAt,
            NodeName = "dead-node",
            Attempt = 1,
            Progress = 0
        };

        await harness.Store.CreateRunsAsync([stale], cancellationToken: ct);

        var recovered = await TestWait.AwaitRunAsync(
            harness.Store, harness.Notifications, stale.Id,
            run => run.Status == JobStatus.Failed,
            TimeSpan.FromSeconds(8),
            "Timed out waiting for stale running run to dead-letter when retries are exhausted.",
            ct);

        Assert.Equal(JobStatus.Failed, recovered.Status);
    }

    [Fact]
    public async Task ContinuousJob_DoesNotCreateExtraRuns_WhenManualBacklogAlreadySatisfiesMax()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"ContinuousBacklog_{Guid.CreateVersion7():N}";

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMilliseconds(20);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.InactiveThreshold = TimeSpan.FromMilliseconds(250);
                options.RetentionPeriod = null;
            },
            (host, permits) =>
            {
                host.AddJob(jobName, async (CancellationToken ct) =>
                {
                    await permits.Reader.ReadAsync(ct);
                    return 1;
                }).Continuous().WithMaxConcurrency(1);
            });

        await harness.StartAsync(ct);

        await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = jobName,
                ExactJobName = true,
                Status = JobStatus.Running
            }, 0, 10),
            p => p.Items.Count == 1,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for first continuous run to start.",
            ct);

        await harness.Client.TriggerAsync(jobName, cancellationToken: ct);
        await harness.Client.TriggerAsync(jobName, cancellationToken: ct);

        await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = jobName,
                ExactJobName = true,
                IsTerminal = false
            }, 0, 20),
            p => p.Items.Count == 3,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for manual backlog to reach three non-terminal runs.",
            ct);

        await harness.Permits.Writer.WriteAsync(true, ct);

        var settled = await TestWait.PollUntilAsync(
            async () => await harness.Store.GetRunsAsync(new()
            {
                JobName = jobName,
                ExactJobName = true,
                IsTerminal = false
            }, 0, 20),
            p => p.Items.Count == 2 && p.Items.Count(r => r.Status == JobStatus.Running) == 1,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for backlog to drain without over-seeding.",
            ct);

        Assert.Equal(2, settled.Items.Count);
    }

    [Fact]
    public async Task JobTimeout_TransitionsRunToFailed()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TimeoutFail_{Guid.CreateVersion7():N}";

        var jobStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMilliseconds(20);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.InactiveThreshold = TimeSpan.FromMilliseconds(250);
                options.RetentionPeriod = null;
            },
            (host, _) =>
            {
                host.AddJob(jobName, async (CancellationToken ct) =>
                {
                    jobStarted.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                    return 1;
                }).WithTimeout(TimeSpan.FromSeconds(5));
            });

        await harness.StartAsync(ct);

        var jobRun = await harness.Client.TriggerAsync(jobName, cancellationToken: ct);

        await jobStarted.Task.WaitAsync(TimeSpan.FromSeconds(60), ct);

        var run = await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(jobRun.Id, ct),
            r => r.Status == JobStatus.Failed,
            TimeSpan.FromSeconds(60),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for timeout run to reach Failed status.",
            ct);
        Assert.Null(run.Reason);
        Assert.NotNull(run.CompletedAt);

        var failures =
            await harness.Store.GetEventsAsync(run.Id, 0, [RunEventType.AttemptFailure], cancellationToken: ct);
        var failure = Assert.Single(failures);
        Assert.NotNull(failure.Payload);
        Assert.Contains("timed out", failure.Payload, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task JobTimeout_WithRetry_RetriesAfterTimeout()
    {
        var ct = TestContext.Current.CancellationToken;
        var jobName = $"TimeoutRetry_{Guid.CreateVersion7():N}";
        var attempts = 0;

        var jobStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMilliseconds(20);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.InactiveThreshold = TimeSpan.FromMilliseconds(250);
                options.RetentionPeriod = null;
            },
            (host, _) =>
            {
                host.AddJob(jobName, async (CancellationToken ct) =>
                    {
                        if (Interlocked.Increment(ref attempts) == 1)
                        {
                            jobStarted.TrySetResult();
                            // First attempt times out
                            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                        }

                        // Second attempt succeeds
                        return 42;
                    }).WithTimeout(TimeSpan.FromSeconds(5))
                    .WithRetry(policy =>
                    {
                        policy.MaxRetries = 1;
                        policy.InitialDelay = TimeSpan.FromMilliseconds(50);
                    });
            });

        await harness.StartAsync(ct);

        var jobRun = await harness.Client.TriggerAsync(jobName, cancellationToken: ct);

        await jobStarted.Task.WaitAsync(TimeSpan.FromSeconds(60), ct);

        var run = await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(jobRun.Id, ct),
            r => r.Status == JobStatus.Succeeded,
            TimeSpan.FromSeconds(60),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for timeout-retry run to succeed on second attempt.",
            ct);
        Assert.Equal(2, run.Attempt);
    }

    private async Task<RuntimeHarness> CreateHarnessAsync(Action<SurefireOptions> configure,
        Action<IHost, Channel<bool>> registerJobs)
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSurefire(configure);
        services.AddSingleton(Store);
        services.AddSingleton<IJobStore>(_ => Store);
        var permits = Channel.CreateUnbounded<bool>();
        services.AddSingleton(permits);

        var provider = services.BuildServiceProvider();
        var host = new TestHost(provider);
        var harnessPermits = provider.GetRequiredService<Channel<bool>>();
        registerJobs(host, harnessPermits);

        var client = provider.GetRequiredService<IJobClient>();
        var notifications = provider.GetRequiredService<INotificationProvider>();

        var hostedServices = provider.GetServices<IHostedService>().ToArray();
        return new(provider, host, Store, notifications, client, harnessPermits, hostedServices, false);
    }
}