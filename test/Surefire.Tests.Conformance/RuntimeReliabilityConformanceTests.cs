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

        await harness.StartAsync();

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
            "Timed out waiting for startup continuous seeding to create max-concurrency runs.");

        Assert.Equal(3, page.Items.Count);
    }

    [Fact]
    public async Task StaleRunningRun_IsRecoveredAndReclaimed_ByMaintenanceLoop()
    {
        var jobName = $"CrashRecovery_{Guid.CreateVersion7():N}";

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMilliseconds(20);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.InactiveThreshold = TimeSpan.FromMilliseconds(120);
                options.RetentionPeriod = null;
            },
            (host, _) => host.AddJob(jobName, () => 7).WithRetry(1));

        await harness.StartAsync();

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

        await harness.Store.CreateRunsAsync([stale]);

        var recovered = await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(stale.Id),
            run => run.Status == JobStatus.Completed,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for stale running run to be recovered and completed.");

        Assert.NotNull(recovered);
        Assert.Equal(2, recovered.Attempt);
    }

    [Fact]
    public async Task StaleRunningRun_ExhaustedRetryPolicy_TransitionsToDeadLetter()
    {
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

        await harness.StartAsync();

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

        await harness.Store.CreateRunsAsync([stale]);

        var recovered = await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(stale.Id),
            run => run.Status == JobStatus.DeadLetter,
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for stale running run to dead-letter when retries are exhausted.");

        Assert.NotNull(recovered);
        Assert.Equal(JobStatus.DeadLetter, recovered.Status);
    }

    [Fact]
    public async Task ContinuousJob_DoesNotCreateExtraRuns_WhenManualBacklogAlreadySatisfiesMax()
    {
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

        await harness.StartAsync();

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
            "Timed out waiting for first continuous run to start.");

        await harness.Client.TriggerAsync(jobName);
        await harness.Client.TriggerAsync(jobName);

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
            "Timed out waiting for manual backlog to reach three non-terminal runs.");

        await harness.Permits.Writer.WriteAsync(true);

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
            "Timed out waiting for backlog to drain without over-seeding.");

        Assert.Equal(2, settled.Items.Count);
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

        var hostedServices = provider.GetServices<IHostedService>().ToArray();
        return new(provider, host, Store, client, harnessPermits, hostedServices, disposeProvider: false);
    }
}