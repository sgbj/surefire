using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireMigrationService(
    IJobStore store,
    JobRegistry registry,
    TimeProvider timeProvider,
    SurefireOptions options,
    ILogger<SurefireMigrationService> logger) : IHostedLifecycleService
{
    public async Task StartingAsync(CancellationToken cancellationToken)
    {
        logger.LogInformation("Surefire initializing...");

        if (options.AutoMigrate)
        {
            await store.MigrateAsync(cancellationToken);
        }

        foreach (var name in registry.GetJobNames())
        {
            var job = registry.Get(name);
            if (job is not null)
                await store.UpsertJobAsync(job.Definition, cancellationToken);
        }

        var now = timeProvider.GetUtcNow();
        var nodeName = options.NodeName;
        var node = new NodeInfo
        {
            Name = nodeName,
            StartedAt = now,
            LastHeartbeatAt = now,
            Status = NodeStatus.Online,
            RegisteredJobNames = [.. registry.GetJobNames()]
        };
        await store.RegisterNodeAsync(node, cancellationToken);

        logger.LogInformation("Surefire node {NodeName} registered", nodeName);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
