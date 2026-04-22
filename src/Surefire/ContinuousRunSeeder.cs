namespace Surefire;

internal static class ContinuousRunSeeder
{
    public static async Task EnsureCapacityAsync(IJobStore store, INotificationProvider notifications,
        TimeProvider timeProvider, JobDefinition definition, CancellationToken cancellationToken)
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