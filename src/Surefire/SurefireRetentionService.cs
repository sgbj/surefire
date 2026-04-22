using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireRetentionService(
    IJobStore store,
    SurefireOptions options,
    TimeProvider timeProvider,
    SurefireInstrumentation instrumentation,
    LoopHealthTracker loopHealth,
    Backoff backoff,
    ILogger<SurefireRetentionService> logger) : BackgroundService
{
    internal const string LoopName = "retention";
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(30);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        loopHealth.Register(LoopName, options.RetentionCheckInterval);
        using var timer = new PeriodicTimer(options.RetentionCheckInterval, timeProvider);

        try
        {
            var backoffAttempt = 0;
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    await RunRetentionAsync(stoppingToken);
                    loopHealth.RecordSuccess(LoopName);
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

                    // A job scheduler should never crash the host on a single bad tick. Log the
                    // exception, record instrumentation, back off and continue so operators see
                    // the failure via metrics/health-check while the host stays up.
                    Log.RetentionTickFailed(logger, ex);
                    instrumentation.RecordLoopError(LoopName);
                    loopHealth.RecordFailure(LoopName);
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