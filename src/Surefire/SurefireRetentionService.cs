using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireRetentionService(
    IJobStore store,
    SurefireOptions options,
    TimeProvider timeProvider,
    ILogger<SurefireRetentionService> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.RetentionCheckInterval, timeProvider);

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    if (options.RetentionPeriod is { } retention)
                    {
                        var threshold = timeProvider.GetUtcNow() - retention;
                        await store.PurgeAsync(threshold, stoppingToken);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.RetentionTickFailed(logger, ex);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1401, Level = LogLevel.Error, Message = "Retention tick failed.")]
        public static partial void RetentionTickFailed(ILogger logger, Exception exception);
    }
}