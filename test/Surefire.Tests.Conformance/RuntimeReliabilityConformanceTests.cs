using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

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

        var page = await PollUntilAsync(
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

        var recovered = await PollUntilAsync(
            () => harness.Store.GetRunAsync(stale.Id),
            run => run is { } && run.Status == JobStatus.Completed,
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

        var recovered = await PollUntilAsync(
            () => harness.Store.GetRunAsync(stale.Id),
            run => run is { } && run.Status == JobStatus.DeadLetter,
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

        await PollUntilAsync(
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

        await PollUntilAsync(
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

        var settled = await PollUntilAsync(
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
        return new(provider, host, Store, client, harnessPermits, hostedServices);
    }

    private static async Task<T> PollUntilAsync<T>(Func<Task<T>> probe, Func<T, bool> condition,
        TimeSpan timeout, TimeSpan interval, string timeoutMessage)
    {
        var deadline = DateTimeOffset.UtcNow + timeout;
        while (DateTimeOffset.UtcNow < deadline)
        {
            var value = await probe();
            if (condition(value))
            {
                return value;
            }

            await Task.Delay(interval);
        }

        throw new TimeoutException(timeoutMessage);
    }

    private sealed class TestHost(IServiceProvider services) : IHost
    {
        public IServiceProvider Services { get; } = services;

        public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Dispose()
        {
            if (Services is IDisposable disposable)
            {
                disposable.Dispose();
            }
        }
    }

    private sealed class RuntimeHarness(
        ServiceProvider provider,
        IHost host,
        IJobStore store,
        IJobClient client,
        Channel<bool> permits,
        IReadOnlyList<IHostedService> hostedServices) : IAsyncDisposable
    {
        public ServiceProvider Provider { get; } = provider;
        public IHost Host { get; } = host;
        public IJobStore Store { get; } = store;
        public IJobClient Client { get; } = client;
        public Channel<bool> Permits { get; } = permits;

        public async ValueTask DisposeAsync()
        {
            foreach (var service in hostedServices.Reverse())
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StoppingAsync(CancellationToken.None);
                }
            }

            foreach (var service in hostedServices.Reverse())
            {
                await service.StopAsync(CancellationToken.None);
            }

            foreach (var service in hostedServices.Reverse())
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StoppedAsync(CancellationToken.None);
                }
            }
        }

        public async Task StartAsync()
        {
            foreach (var service in hostedServices)
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StartingAsync(CancellationToken.None);
                }
            }

            foreach (var service in hostedServices)
            {
                await service.StartAsync(CancellationToken.None);
            }

            foreach (var service in hostedServices)
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StartedAsync(CancellationToken.None);
                }
            }
        }
    }
}