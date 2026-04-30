using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Surefire.Tests.Testing;

namespace Surefire.Tests.PostgreSql;

[Collection("PostgreSql")]
public sealed class PostgreSqlRuntimeTests(PostgreSqlFixture fixture)
{
    [Fact]
    public async Task OwnedFireAndForgetChildren_ParentDeadLetter_CancelsChildren_WithPostgreSqlNotifications()
    {
        var ct = TestContext.Current.CancellationToken;
        var parentJobName = $"PgDeadLetterOwner_{Guid.CreateVersion7():N}";
        var childJobName = $"PgDeadLetterOwnedChild_{Guid.CreateVersion7():N}";

        var services = new ServiceCollection();
        services.AddLogging();
        services.AddSingleton(fixture.DataSource);
        services.AddSurefire(options =>
        {
            options.AutoMigrate = false;
            options.PollingInterval = TimeSpan.FromMinutes(1);
            options.HeartbeatInterval = TimeSpan.FromMilliseconds(40);
            options.RetentionPeriod = null;
            options.MaxNodeConcurrency = 4;
            options.UsePostgreSql(_ => fixture.DataSource);
        });

        await using var provider = services.BuildServiceProvider();
        var host = new TestHost(provider);
        host.AddJob(childJobName, async (CancellationToken ct) =>
        {
            await Task.Delay(TimeSpan.FromSeconds(30), ct);
            return 1;
        });

        host.AddJob(parentJobName, async (IJobClient client, JobContext context, CancellationToken ct) =>
        {
            for (var i = 0; i < 3; i++)
            {
                var child = await client.TriggerAsync(childJobName, cancellationToken: ct);
                Assert.Equal(context.RunId, child.ParentRunId);
            }

            await TestWait.PollUntilConditionAsync(
                async _ =>
                {
                    var page = await provider.GetRequiredService<IJobStore>().GetRunsAsync(
                        new() { ParentRunId = context.RunId, JobName = childJobName },
                        0,
                        10,
                        ct);

                    return page.Items.Count == 3 && page.Items.All(r => r.Status == JobStatus.Running);
                },
                TimeSpan.FromSeconds(10),
                TimeSpan.FromMilliseconds(25),
                "Timed out waiting for PostgreSQL children to be running before the owner failed.",
                ct);

            throw new InvalidOperationException("owner failed after starting PostgreSQL children");
        }).WithRetry(0);

        var hostedServices = provider.GetServices<IHostedService>().ToArray();
        await StartHostedServicesAsync(hostedServices, ct);
        try
        {
            var client = provider.GetRequiredService<IJobClient>();
            var store = provider.GetRequiredService<IJobStore>();
            var parent = await client.TriggerAsync(parentJobName, cancellationToken: ct);

            await TestWait.PollUntilAsync(
                () => store.GetRunAsync(parent.Id, ct),
                run => run?.Status == JobStatus.Failed,
                TimeSpan.FromSeconds(10),
                TimeSpan.FromMilliseconds(25),
                "Timed out waiting for the PostgreSQL owner to dead-letter.",
                ct);

            var children = await TestWait.PollUntilAsync(
                async () =>
                {
                    var page = await store.GetRunsAsync(
                        new() { ParentRunId = parent.Id, JobName = childJobName },
                        0,
                        10,
                        ct);

                    return page.Items;
                },
                runs => runs.Count == 3 && runs.All(r => r.Status == JobStatus.Canceled),
                TimeSpan.FromSeconds(10),
                TimeSpan.FromMilliseconds(25),
                "Timed out waiting for PostgreSQL dead-lettered owner children to be Canceled.",
                ct);

            Assert.All(children, child =>
            {
                Assert.Equal(parent.Id, child.ParentRunId);
                Assert.Equal($"Canceled because parent run '{parent.Id}' failed.", child.Reason);
            });
        }
        finally
        {
            await StopHostedServicesAsync(hostedServices);
        }
    }

    private static async Task StartHostedServicesAsync(IReadOnlyList<IHostedService> hostedServices,
        CancellationToken cancellationToken)
    {
        foreach (var service in hostedServices)
        {
            if (service is IHostedLifecycleService lifecycle)
            {
                await lifecycle.StartingAsync(cancellationToken);
            }
        }

        foreach (var service in hostedServices)
        {
            await service.StartAsync(cancellationToken);
        }

        foreach (var service in hostedServices)
        {
            if (service is IHostedLifecycleService lifecycle)
            {
                await lifecycle.StartedAsync(cancellationToken);
            }
        }
    }

    private static async Task StopHostedServicesAsync(IReadOnlyList<IHostedService> hostedServices)
    {
        for (var i = hostedServices.Count - 1; i >= 0; i--)
        {
            if (hostedServices[i] is IHostedLifecycleService lifecycle)
            {
                await lifecycle.StoppingAsync(CancellationToken.None);
            }
        }

        for (var i = hostedServices.Count - 1; i >= 0; i--)
        {
            await hostedServices[i].StopAsync(CancellationToken.None);
        }

        for (var i = hostedServices.Count - 1; i >= 0; i--)
        {
            if (hostedServices[i] is IHostedLifecycleService lifecycle)
            {
                await lifecycle.StoppedAsync(CancellationToken.None);
            }
        }
    }
}
