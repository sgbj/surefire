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
    public async Task OwnedFireAndForgetChildren_ParentDeadLetter_CancelsChildren()
    {
        var ct = TestContext.Current.CancellationToken;
        var parentJobName = $"DeadLetterOwner_{Guid.CreateVersion7():N}";
        var childJobName = $"DeadLetterOwnedChild_{Guid.CreateVersion7():N}";

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMinutes(1);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.RetentionPeriod = null;
                options.MaxNodeConcurrency = 4;
            },
            (host, _) =>
            {
                host.AddJob(childJobName, async (CancellationToken ct) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(30), ct);
                    return 1;
                });

                host.AddJob(parentJobName, async (IJobClient client, JobContext context, CancellationToken ct) =>
                {
                    for (var i = 0; i < 3; i++)
                    {
                        await client.TriggerAsync(childJobName, cancellationToken: ct);
                    }

                    await TestWait.PollUntilConditionAsync(
                        async _ =>
                        {
                            var page = await Store.GetRunsAsync(
                                new() { ParentRunId = context.RunId, JobName = childJobName },
                                0,
                                10,
                                ct);

                            return page.Items.Count == 3 && page.Items.All(r => r.Status == JobStatus.Running);
                        },
                        TimeSpan.FromSeconds(10),
                        TimeSpan.FromMilliseconds(25),
                        "Timed out waiting for children to be running before the owner failed.",
                        ct);

                    throw new InvalidOperationException("owner failed after starting children");
                }).WithRetry(0);
            });

        await harness.StartAsync(ct);

        var parent = await harness.Client.TriggerAsync(parentJobName, cancellationToken: ct);

        await TestWait.PollUntilAsync(
            () => harness.Store.GetRunAsync(parent.Id, ct),
            run => run?.Status == JobStatus.Failed,
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for the owner to dead-letter.",
            ct);

        var children = await TestWait.PollUntilAsync(
            async () =>
            {
                var page = await harness.Store.GetRunsAsync(
                    new() { ParentRunId = parent.Id, JobName = childJobName },
                    0,
                    10,
                    ct);

                return page.Items;
            },
            runs => runs.Count == 3 && runs.All(r => r.Status == JobStatus.Canceled),
            TimeSpan.FromSeconds(10),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for dead-lettered owner children to be Canceled.",
            ct);

        Assert.All(children, child =>
        {
            Assert.Equal(parent.Id, child.ParentRunId);
            Assert.Equal($"Canceled because parent run '{parent.Id}' failed.", child.Reason);
        });
    }

    [Fact]
    public async Task StaleRunningRun_ExhaustedRetryPolicy_CancelsOwnedChildren()
    {
        var ct = TestContext.Current.CancellationToken;
        var parentJobName = $"StaleDeadLetterOwner_{Guid.CreateVersion7():N}";
        var childJobName = $"StaleDeadLetterOwnedChild_{Guid.CreateVersion7():N}";

        await using var harness = await CreateHarnessAsync(options =>
            {
                options.AutoMigrate = false;
                options.PollingInterval = TimeSpan.FromMilliseconds(20);
                options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
                options.InactiveThreshold = TimeSpan.FromSeconds(1);
                options.RetentionPeriod = null;
            },
            (host, _) =>
            {
                host.AddJob(parentJobName, () => 1).WithRetry(0);
                host.AddJob(childJobName, async (CancellationToken ct) =>
                {
                    await Task.Delay(TimeSpan.FromSeconds(30), ct);
                    return 1;
                });
            });

        await harness.StartAsync(ct);

        var staleAt = DateTimeOffset.UtcNow.AddSeconds(-10);
        var now = DateTimeOffset.UtcNow;
        var parent = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = parentJobName,
            Status = JobStatus.Running,
            CreatedAt = staleAt,
            NotBefore = staleAt,
            StartedAt = staleAt,
            LastHeartbeatAt = staleAt,
            NodeName = "dead-node",
            Attempt = 1,
            Progress = 0
        };

        var children = Enumerable.Range(0, 3)
            .Select(_ => new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = childJobName,
                Status = JobStatus.Running,
                CreatedAt = now,
                NotBefore = now,
                StartedAt = now,
                LastHeartbeatAt = now,
                NodeName = "child-node",
                Attempt = 1,
                Progress = 0,
                ParentRunId = parent.Id,
                RootRunId = parent.Id
            })
            .ToArray();

        await harness.Store.CreateRunsAsync([parent, .. children], cancellationToken: ct);

        await TestWait.AwaitRunAsync(
            harness.Store,
            harness.Notifications,
            parent.Id,
            run => run.Status == JobStatus.Failed,
            TimeSpan.FromSeconds(8),
            "Timed out waiting for stale owner to dead-letter.",
            ct);

        var storedChildren = await TestWait.PollUntilAsync(
            async () =>
            {
                var page = await harness.Store.GetRunsAsync(
                    new() { ParentRunId = parent.Id, JobName = childJobName },
                    0,
                    10,
                    ct);

                return page.Items;
            },
            runs => runs.Count == 3 && runs.All(r => r.Status == JobStatus.Canceled),
            TimeSpan.FromSeconds(8),
            TimeSpan.FromMilliseconds(25),
            "Timed out waiting for stale owner children to be Canceled.",
            ct);

        Assert.All(storedChildren, child =>
        {
            Assert.Equal(parent.Id, child.ParentRunId);
            Assert.Equal($"Canceled because parent run '{parent.Id}' failed.", child.Reason);
        });
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
                            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
                        }

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
