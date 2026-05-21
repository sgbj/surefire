namespace Surefire;

internal static class ContinuousRunSeeder
{
    public static async Task EnsureCapacityAsync(IJobStore store, INotificationProvider notifications,
        TimeProvider timeProvider, SurefireOptions options, JobDefinition definition, CancellationToken cancellationToken)
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
                ExpiresAt = JobRunDefaults.GetDefaultExpiresAt(now, options),
                Priority = definition.Priority,
                Progress = 0,
                Attempt = 1
            };

            bool created;
            try
            {
                created = await store.TryCreateRunAsync(
                    run,
                    desired,
                    cancellationToken: cancellationToken);
            }
            catch (RunConflictException)
            {
                break;
            }

            if (!created)
            {
                break;
            }

            await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);
        }
    }
}
