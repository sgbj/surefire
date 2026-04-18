using Cronos;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireSchedulerService(
    IJobStore store,
    INotificationProvider notifications,
    SurefireOptions options,
    TimeProvider timeProvider,
    SurefireInstrumentation instrumentation,
    Backoff backoff,
    ILogger<SurefireSchedulerService> logger) : BackgroundService
{
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.PollingInterval, timeProvider);

        try
        {
            var backoffAttempt = 0;
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await ScheduleDueJobsAsync(stoppingToken);
                    backoffAttempt = 0;
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    if (stoppingToken.IsCancellationRequested)
                    {
                        break;
                    }

                    Log.SchedulerTickFailed(logger, ex);
                    if (!store.IsTransientException(ex))
                    {
                        throw;
                    }

                    instrumentation.RecordStoreRetry("scheduler");
                    await Task.Delay(backoff.NextDelay(backoffAttempt++, BackoffInitial, BackoffMax), timeProvider,
                        stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    internal async Task ScheduleDueJobsAsync(CancellationToken cancellationToken)
    {
        var jobs = await store.GetJobsAsync(new() { IsEnabled = true }, cancellationToken);
        var now = timeProvider.GetUtcNow();

        foreach (var job in jobs)
        {
            if (string.IsNullOrWhiteSpace(job.CronExpression))
            {
                continue;
            }

            if (!CronScheduleValidation.TryParseCron(job.CronExpression, out var cron))
            {
                Log.SkippingJobDueToInvalidCronExpression(
                    logger,
                    job.Name,
                    job.CronExpression);
                continue;
            }

            if (!CronScheduleValidation.TryResolveTimeZone(job.TimeZoneId, out var timeZone))
            {
                Log.SkippingJobDueToInvalidTimeZone(logger, job.Name, job.TimeZoneId);
                continue;
            }

            if (job.LastCronFireAt is null)
            {
                await store.UpdateLastCronFireAtAsync(job.Name, now, cancellationToken);
                continue;
            }

            switch (job.MisfirePolicy)
            {
                case MisfirePolicy.Skip:
                    // Advance LastCronFireAt to the most recent missed occurrence so the dashboard
                    // reflects the actual last fire and the next tick resumes from the right place.
                    // Iterate without allocating a list — we only keep the latest.
                    if (FindLatestMissedOccurrence(cron, timeZone, job.LastCronFireAt.Value, now) is { } skipTo)
                    {
                        await store.UpdateLastCronFireAtAsync(job.Name, skipTo, cancellationToken);
                    }

                    break;

                case MisfirePolicy.FireOnce:
                    // Need the latest missed occurrence — the dedup ID is derived from it and
                    // must be deterministic across nodes.
                    if (FindLatestMissedOccurrence(cron, timeZone, job.LastCronFireAt.Value, now) is { } fireAt)
                    {
                        await TryCreateScheduledRunAsync(job, now, fireAt, cancellationToken);
                    }

                    break;

                case MisfirePolicy.FireAll:
                    var fireTimesResult = GetMissedFireTimes(cron, timeZone, job.LastCronFireAt.Value,
                        now, job.FireAllLimit);
                    if (fireTimesResult.FireTimes.Count == 0)
                    {
                        break;
                    }

                    if (fireTimesResult.IsTruncated)
                    {
                        Log.FireAllLimited(logger, job.Name, job.FireAllLimit!.Value);
                    }

                    foreach (var fireTime in fireTimesResult.FireTimes)
                    {
                        await TryCreateScheduledRunAsync(job, fireTime, fireTime, cancellationToken);
                    }

                    break;

                default:
                    throw new ArgumentOutOfRangeException(nameof(job.MisfirePolicy), job.MisfirePolicy,
                        "Unknown misfire policy.");
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

    private static DateTimeOffset? FindLatestMissedOccurrence(CronExpression cron,
        TimeZoneInfo timeZone, DateTimeOffset lastCronFireAt, DateTimeOffset now)
    {
        var cursor = lastCronFireAt;
        DateTimeOffset? latest = null;
        while (cron.GetNextOccurrence(cursor, timeZone) is { } next && next <= now)
        {
            latest = next;
            cursor = next;
        }

        return latest;
    }

    private static MissedFireTimesResult GetMissedFireTimes(CronExpression cron,
        TimeZoneInfo timeZone,
        DateTimeOffset lastCronFireAt,
        DateTimeOffset now,
        int? maxOccurrences)
    {
        var fireTimes = new List<DateTimeOffset>();
        var cursor = lastCronFireAt;
        while (true)
        {
            var next = cron.GetNextOccurrence(cursor, timeZone);
            if (next is null || next > now)
            {
                break;
            }

            if (maxOccurrences is { } max && fireTimes.Count >= max)
            {
                return new(fireTimes, true);
            }

            fireTimes.Add(next.Value);
            cursor = next.Value;
        }

        return new(fireTimes, false);
    }

    private readonly record struct MissedFireTimesResult(IReadOnlyList<DateTimeOffset> FireTimes, bool IsTruncated);

    private static partial class Log
    {
        [LoggerMessage(EventId = 1201, Level = LogLevel.Error, Message = "Scheduler tick failed.")]
        public static partial void SchedulerTickFailed(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 1202, Level = LogLevel.Warning,
            Message = "Skipping job '{JobName}' due to invalid cron expression '{CronExpression}'.")]
        public static partial void SkippingJobDueToInvalidCronExpression(ILogger logger, string jobName,
            string? cronExpression);

        [LoggerMessage(EventId = 1203, Level = LogLevel.Warning,
            Message = "Skipping job '{JobName}' due to invalid time zone '{TimeZoneId}'.")]
        public static partial void SkippingJobDueToInvalidTimeZone(ILogger logger, string jobName, string? timeZoneId);

        [LoggerMessage(EventId = 1204, Level = LogLevel.Warning,
            Message =
                "Job '{JobName}' reached FireAllLimit={FireAllLimit}. Remaining missed fires will be scheduled on subsequent ticks.")]
        public static partial void FireAllLimited(ILogger logger, string jobName, int fireAllLimit);
    }
}