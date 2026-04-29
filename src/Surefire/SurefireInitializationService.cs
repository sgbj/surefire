using Microsoft.Extensions.Hosting;

namespace Surefire;

internal sealed class SurefireInitializationService(
    IJobStore store,
    INotificationProvider notifications,
    JobRegistry registry,
    SurefireOptions options,
    TimeProvider timeProvider) : IHostedLifecycleService
{
    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        if (options.AutoMigrate)
        {
            await store.MigrateAsync(cancellationToken);
        }

        await notifications.InitializeAsync(cancellationToken);

        var registrations = registry.Snapshot();

        await store.UpsertJobsAsync(
            registrations.Select(r => r.Definition).ToList(),
            cancellationToken);

        // Upsert every queue before executors start so claim's queue join sees a real row.
        // Configured queues persist their pause/concurrency/rate-limit settings; registry
        // fallbacks (e.g. implicit "default") get a minimal definition that AddQueue can later
        // overwrite on the next maintenance tick.
        var configuredQueueNames = new HashSet<string>(
            options.Queues.Select(q => q.Name), StringComparer.Ordinal);
        var queuesToUpsert = new List<QueueDefinition>(options.Queues);
        foreach (var name in registry.GetQueueNames())
        {
            if (configuredQueueNames.Contains(name))
            {
                continue;
            }

            queuesToUpsert.Add(new() { Name = name });
        }

        await store.UpsertQueuesAsync(queuesToUpsert, cancellationToken);
        await store.UpsertRateLimitsAsync(options.RateLimits.ToList(), cancellationToken);

        foreach (var registered in registrations)
        {
            await ContinuousRunSeeder.EnsureCapacityAsync(
                store,
                notifications,
                timeProvider,
                registered.Definition,
                cancellationToken);
        }

        await store.HeartbeatAsync(
            options.NodeName,
            registry.GetJobNames(),
            registry.GetQueueNames(),
            [],
            cancellationToken);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
