using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class SurefireMigrationService(
    IJobStore store,
    RunRecovery recovery,
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
            RegisteredJobNames = [.. registry.GetJobNames()]
        };
        await store.RegisterNodeAsync(node, cancellationToken);

        // Startup orphan recovery: recover stale runs on this node name
        try
        {
            var staleRuns = await store.GetStaleRunsAsync(options.StaleNodeThreshold, cancellationToken);
            foreach (var run in staleRuns.Where(r => r.NodeName == nodeName))
            {
                if (await recovery.TryRecoverRunAsync(run, $"Recovered at startup (node '{nodeName}' restarted)", cancellationToken))
                    logger.LogInformation("Recovered orphaned run {RunId} at startup", run.Id);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to recover orphaned runs at startup");
        }

        logger.LogInformation("Surefire node {NodeName} registered", nodeName);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public async Task StoppedAsync(CancellationToken cancellationToken)
    {
        try
        {
            // Use a short timeout rather than the host's cancellation token,
            // which may already be cancelled by the time StoppedAsync is called.
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await store.RemoveNodeAsync(options.NodeName, cts.Token);
            logger.LogInformation("Surefire node {NodeName} deregistered", options.NodeName);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to deregister node {NodeName}", options.NodeName);
        }
    }
}
