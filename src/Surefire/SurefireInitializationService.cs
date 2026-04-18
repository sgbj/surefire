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

        foreach (var registered in registrations)
        {
            await store.UpsertJobAsync(registered.Definition, cancellationToken);
        }

        foreach (var queue in options.Queues)
        {
            await store.UpsertQueueAsync(queue, cancellationToken);
        }

        foreach (var rateLimit in options.RateLimits)
        {
            await store.UpsertRateLimitAsync(rateLimit, cancellationToken);
        }

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