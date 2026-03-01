using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireSchedulerService(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options,
    TimeProvider timeProvider,
    ILogger<SurefireSchedulerService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.PollingInterval);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await ScheduleDueJobsAsync(stoppingToken);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.SchedulerTickFailed(logger, ex);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task ScheduleDueJobsAsync(CancellationToken cancellationToken)
    {
        var jobs = await store.GetJobsAsync(new() { IsEnabled = true }, cancellationToken);
        var now = timeProvider.GetUtcNow();

        foreach (var job in jobs)
        {
            if (string.IsNullOrWhiteSpace(job.CronExpression))
            {
                continue;
            }

            if (!TryGetCron(job.CronExpression, out var cron))
            {
                Log.SkippingJobDueToInvalidCronExpression(
                    logger,
                    job.Name,
                    job.CronExpression);
                continue;
            }

            var timeZone = ResolveTimeZone(job.TimeZoneId);
            if (job.LastCronFireAt is null)
            {
                await store.UpdateLastCronFireAtAsync(job.Name, now, cancellationToken);
                continue;
            }

            var fireTimes = GetMissedFireTimes(cron, timeZone, job.LastCronFireAt, now);
            if (fireTimes.Count == 0)
            {
                continue;
            }

            switch (job.MisfirePolicy)
            {
                case MisfirePolicy.Skip:
                    await store.UpdateLastCronFireAtAsync(job.Name, fireTimes[^1], cancellationToken);
                    break;

                case MisfirePolicy.FireOnce:
                    await TryCreateScheduledRunAsync(job, now, fireTimes[^1], cancellationToken);
                    break;

                case MisfirePolicy.FireAll:
                    foreach (var fireTime in fireTimes)
                    {
                        await TryCreateScheduledRunAsync(job, fireTime, fireTime, cancellationToken);
                    }

                    break;

                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    private async Task TryCreateScheduledRunAsync(JobDefinition job, DateTimeOffset notBefore,
        DateTimeOffset cronFireAt, CancellationToken cancellationToken)
    {
        var run = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = job.Name,
            Status = JobStatus.Pending,
            CreatedAt = timeProvider.GetUtcNow(),
            NotBefore = notBefore,
            NotAfter = null,
            Priority = job.Priority,
            QueuePriority = 0,
            DeduplicationId = BuildCronDeduplicationId(job.Name, cronFireAt),
            Progress = 0,
            Attempt = 0
        };

        var created = await store.TryCreateRunAsync(
            run,
            lastCronFireAt: cronFireAt,
            cancellationToken: cancellationToken);

        if (!created)
        {
            return;
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);
    }

    private static string BuildCronDeduplicationId(string jobName, DateTimeOffset fireTime) =>
        $"cron:{jobName}:{fireTime.UtcTicks}";

    private static bool TryGetCron(string cronExpression, out CronExpression cron)
    {
        try
        {
            var fields = cronExpression.Split(' ',
                StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            cron = fields.Length == 6
                ? CronExpression.Parse(cronExpression, CronFormat.IncludeSeconds)
                : CronExpression.Parse(cronExpression, CronFormat.Standard);
            return true;
        }
        catch
        {
            cron = null!;
            return false;
        }
    }

    private TimeZoneInfo ResolveTimeZone(string? timeZoneId)
    {
        if (string.IsNullOrWhiteSpace(timeZoneId))
        {
            return TimeZoneInfo.Utc;
        }

        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
        }
        catch
        {
            Log.TimeZoneNotFoundFallingBackToUtc(logger, timeZoneId);
            return TimeZoneInfo.Utc;
        }
    }

    private static IReadOnlyList<DateTimeOffset> GetMissedFireTimes(CronExpression cron,
        TimeZoneInfo timeZone,
        DateTimeOffset? lastCronFireAt,
        DateTimeOffset now)
    {
        if (lastCronFireAt is null)
        {
            return [];
        }

        var fireTimes = new List<DateTimeOffset>();
        var cursor = lastCronFireAt.Value;
        while (true)
        {
            var next = cron.GetNextOccurrence(cursor, timeZone);
            if (next is null || next > now)
            {
                break;
            }

            fireTimes.Add(next.Value);
            cursor = next.Value;
        }

        return fireTimes;
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1201, Level = LogLevel.Error, Message = "Scheduler tick failed.")]
        public static partial void SchedulerTickFailed(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 1202, Level = LogLevel.Warning,
            Message = "Skipping job '{JobName}' due to invalid cron expression '{CronExpression}'.")]
        public static partial void SkippingJobDueToInvalidCronExpression(ILogger logger, string jobName,
            string? cronExpression);

        [LoggerMessage(EventId = 1203, Level = LogLevel.Warning,
            Message = "Time zone '{TimeZoneId}' not found. Falling back to UTC.")]
        public static partial void TimeZoneNotFoundFallingBackToUtc(ILogger logger, string? timeZoneId);
    }
}