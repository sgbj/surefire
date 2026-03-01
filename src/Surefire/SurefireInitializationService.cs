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
            await EnsureContinuousSeedCapacityAsync(registered.Definition, cancellationToken);
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

    private async Task EnsureContinuousSeedCapacityAsync(JobDefinition definition, CancellationToken cancellationToken)
    {
        if (!definition.IsContinuous || !definition.IsEnabled)
        {
            return;
        }

        var desired = Math.Max(definition.MaxConcurrency ?? 1, 1);
        for (var i = 0; i < desired; i++)
        {
            var now = timeProvider.GetUtcNow();
            var run = new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = definition.Name,
                Status = JobStatus.Pending,
                CreatedAt = now,
                NotBefore = now,
                Priority = definition.Priority,
                QueuePriority = 0,
                Progress = 0,
                Attempt = 0
            };

            var created = await store.TryCreateRunAsync(
                run,
                desired,
                cancellationToken: cancellationToken);
            if (!created)
            {
                break;
            }

            await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);
        }
    }
}