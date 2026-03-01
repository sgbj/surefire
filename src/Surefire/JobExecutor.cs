using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed class JobExecutor(
    IJobStore store,
    INotificationProvider notifications,
    JobRegistry registry,
    IServiceProvider rootProvider,
    SurefireOptions options,
    TimeProvider timeProvider,
    ILogger<JobExecutor> logger)
{
    public async Task ExecuteAsync(JobRun run, CancellationToken shutdownToken)
    {
        var registered = registry.Get(run.JobName);
        if (registered is null)
        {
            logger.LogWarning("No handler registered for job {JobName}", run.JobName);
            run.Status = JobStatus.Failed;
            run.Error = $"No handler registered for job '{run.JobName}'";
            run.CompletedAt = timeProvider.GetUtcNow();
            await store.UpdateRunAsync(run);
            var payload = JsonSerializer.Serialize(
                new { run.Status, run.Result, run.Error, WillRetry = false },
                options.SerializerOptions);
            await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), payload);
            return;
        }

        using var activity = SurefireActivitySource.StartJobExecution(run.JobName, run.Id, run.NodeName ?? "unknown");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);

        if (registered.Definition.Timeout is { } timeout)
            cts.CancelAfter(timeout);

        var cancelSub = await notifications.SubscribeAsync(
            NotificationChannels.RunCancelled(run.Id),
            _ => { cts.Cancel(); return Task.CompletedTask; });

        await using (cancelSub)
        {
            await using var scope = rootProvider.CreateAsyncScope();
            var sp = scope.ServiceProvider;

            var ctx = new JobContext
            {
                RunId = run.Id,
                JobName = run.JobName,
                CancellationToken = cts.Token,
                Attempt = run.Attempt,
                Store = store,
                Notifications = notifications,
                Run = run
            };

            JobContext.Current.Value = ctx;
            SurefireMetrics.ActiveRuns.Add(1);

            run.StartedAt = timeProvider.GetUtcNow();
            run.TraceId = activity?.TraceId.ToString();
            run.SpanId = activity?.SpanId.ToString();
            await store.UpdateRunAsync(run);

            var start = Stopwatch.GetTimestamp();
            var willRetry = false;

            try
            {
                var result = await registered.CompiledHandler(sp, ctx, run.Arguments);
                if (registered.HasReturnValue)
                    run.Result = JsonSerializer.Serialize(result, options.SerializerOptions);

                var elapsed = Stopwatch.GetElapsedTime(start);
                run.Status = JobStatus.Completed;
                run.CompletedAt = timeProvider.GetUtcNow();
                run.Progress = 1;
                await store.UpdateRunAsync(run);

                SurefireMetrics.JobsExecuted.Add(1);
                SurefireMetrics.JobDuration.Record(elapsed.TotalMilliseconds);

                ctx.Result = run.Result is not null
                    ? JsonSerializer.Deserialize<object>(run.Result, options.SerializerOptions)
                    : null;
                await InvokeCallbacksAsync(options.OnSuccessCallbacks, sp, ctx, "OnSuccess", run.JobName);
                await InvokeCallbacksAsync(registered.OnSuccessCallbacks, sp, ctx, "OnSuccess", run.JobName);
            }
            catch (OperationCanceledException) when (run.CancelledAt is not null || cts.IsCancellationRequested)
            {
                run.Status = JobStatus.Cancelled;
                run.CancelledAt ??= timeProvider.GetUtcNow();
                run.CompletedAt = timeProvider.GetUtcNow();
                await store.UpdateRunAsync(run);
            }
            catch (Exception ex)
            {
                var elapsed = Stopwatch.GetElapsedTime(start);
                SurefireMetrics.JobsFailed.Add(1);
                SurefireMetrics.JobDuration.Record(elapsed.TotalMilliseconds);

                ctx.Exception = ex;

                await InvokeCallbacksAsync(options.OnFailureCallbacks, sp, ctx, "OnFailure", run.JobName);
                await InvokeCallbacksAsync(registered.OnFailureCallbacks, sp, ctx, "OnFailure", run.JobName);

                var retryPolicy = registered.Definition.RetryPolicy;
                if (run.Attempt < retryPolicy.MaxAttempts)
                {
                    willRetry = true;

                    await InvokeCallbacksAsync(options.OnRetryCallbacks, sp, ctx, "OnRetry", run.JobName);
                    await InvokeCallbacksAsync(registered.OnRetryCallbacks, sp, ctx, "OnRetry", run.JobName);

                    var delay = retryPolicy.GetDelay(run.Attempt);
                    var now = timeProvider.GetUtcNow();
                    run.Status = JobStatus.Failed;
                    run.Error = ex.ToString();
                    run.CompletedAt = now;
                    await store.UpdateRunAsync(run);

                    var retryRun = new JobRun
                    {
                        Id = Guid.CreateVersion7().ToString("N"),
                        JobName = run.JobName,
                        Arguments = run.Arguments,
                        Attempt = run.Attempt + 1,
                        CreatedAt = now,
                        NotBefore = now + delay,
                        OriginalRunId = run.OriginalRunId ?? run.Id
                    };
                    await store.CreateRunAsync(retryRun);
                    await notifications.PublishAsync(NotificationChannels.RunCreated, retryRun.Id);
                }
                else
                {
                    run.Status = retryPolicy.MaxAttempts > 1 ? JobStatus.DeadLetter : JobStatus.Failed;
                    run.Error = ex.ToString();
                    run.CompletedAt = timeProvider.GetUtcNow();
                    await store.UpdateRunAsync(run);

                    if (run.Status == JobStatus.DeadLetter)
                    {
                        await InvokeCallbacksAsync(options.OnDeadLetterCallbacks, sp, ctx, "OnDeadLetter", run.JobName);
                        await InvokeCallbacksAsync(registered.OnDeadLetterCallbacks, sp, ctx, "OnDeadLetter", run.JobName);
                    }
                }

                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            }
            finally
            {
                SurefireMetrics.ActiveRuns.Add(-1);
                JobContext.Current.Value = null;
                var completionPayload = JsonSerializer.Serialize(
                    new { run.Status, run.Result, run.Error, WillRetry = willRetry },
                    options.SerializerOptions);
                await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), completionPayload);

                // For retries, also notify the original run's channel on final outcome
                if (run.OriginalRunId is not null && !willRetry)
                {
                    await notifications.PublishAsync(NotificationChannels.RunCompleted(run.OriginalRunId), completionPayload);
                }
            }
        }
    }

    private async Task InvokeCallbacksAsync(
        List<Func<IServiceProvider, JobContext, Task>> callbacks,
        IServiceProvider sp, JobContext ctx,
        string callbackName, string jobName)
    {
        foreach (var callback in callbacks)
        {
            try
            {
                await callback(sp, ctx);
            }
            catch (Exception cbEx)
            {
                logger.LogError(cbEx, "{CallbackName} callback threw for job {JobName}", callbackName, jobName);
            }
        }
    }
}
