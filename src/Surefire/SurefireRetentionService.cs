using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireRetentionService(
    IJobStore store,
    SurefireOptions options,
    TimeProvider timeProvider,
    SurefireInstrumentation instrumentation,
    Backoff backoff,
    ILogger<SurefireRetentionService> logger) : BackgroundService
{
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.RetentionCheckInterval, timeProvider);

        try
        {
            var backoffAttempt = 0;
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await RunRetentionAsync(stoppingToken);
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

                    Log.RetentionTickFailed(logger, ex);
                    if (!store.IsTransientException(ex))
                    {
                        throw;
                    }

                    instrumentation.RecordStoreRetry("retention");
                    await Task.Delay(backoff.NextDelay(backoffAttempt++, BackoffInitial, BackoffMax), timeProvider,
                        stoppingToken);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    internal async Task RunRetentionAsync(CancellationToken cancellationToken)
    {
        if (options.RetentionPeriod is { } retention)
        {
            var threshold = timeProvider.GetUtcNow() - retention;
            await store.PurgeAsync(threshold, cancellationToken);
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1401, Level = LogLevel.Error, Message = "Retention tick failed.")]
        public static partial void RetentionTickFailed(ILogger logger, Exception exception);
    }
}