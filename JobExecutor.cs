using System.Diagnostics;
using System.Diagnostics.Metrics;
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
    SurefireLoggerProvider loggerProvider,
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
            await AppendStatusEventAsync(run);
            await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), "");
            return;
        }

        // Look up parent trace context for distributed tracing
        Activity? parentActivity = null;
        if (run.ParentRunId is not null)
        {
            var parentRun = await store.GetRunAsync(run.ParentRunId, shutdownToken);
            if (parentRun?.TraceId is not null && parentRun.SpanId is not null)
            {
                var parentContext = new ActivityContext(
                    ActivityTraceId.CreateFromString(parentRun.TraceId),
                    ActivitySpanId.CreateFromString(parentRun.SpanId),
                    ActivityTraceFlags.Recorded);
                parentActivity = SurefireActivitySource.StartJobExecution(
                    run.JobName, run.Id, run.NodeName ?? "unknown", parentContext);
            }
        }
        using var activity = parentActivity
            ?? SurefireActivitySource.StartJobExecution(run.JobName, run.Id, run.NodeName ?? "unknown");
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(shutdownToken);

        var hasTimeout = false;
        if (registered.Definition.Timeout is { } timeout)
        {
            cts.CancelAfter(timeout);
            hasTimeout = true;
        }

        // Dedicated flag to distinguish user-cancel from timeout/shutdown.
        // Using run.CancelledAt alone is racy: a concurrent cancel request can set CancelledAt
        // just as a timeout fires, causing the timeout to be misclassified as user-cancel.
        var userCancelled = 0;
        var cancelSub = await notifications.SubscribeAsync(
            NotificationChannels.RunCancel(run.Id),
            _ =>
            {
                Interlocked.Exchange(ref userCancelled, 1);
                run.CancelledAt ??= timeProvider.GetUtcNow();
                try { cts.Cancel(); } catch (ObjectDisposedException) { }
                return Task.CompletedTask;
            });

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
                Run = run,
                TimeProvider = timeProvider
            };

            JobContext.Current.Value = ctx;
            var tags = new TagList { { "surefire.job.name", run.JobName } };
            SurefireMetrics.ActiveRuns.Add(1, tags);

            var now = timeProvider.GetUtcNow();
            run.StartedAt = now;
            run.LastHeartbeatAt = now;
            run.TraceId = activity?.TraceId.ToString();
            run.SpanId = activity?.SpanId.ToString();
            await store.UpdateRunAsync(run);
            await AppendStatusEventAsync(run);

            var start = Stopwatch.GetTimestamp();

            try
            {
                var result = await registered.CompiledHandler(sp, ctx, run.Arguments);

                if (result is not null && StreamingHelper.IsAsyncEnumerable(result.GetType()))
                {
                    try
                    {
                        await EnumerateOutputAsync(run, result, cts.Token);
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex)
                    {
                        await AppendOutputCompleteAsync(run, ex.ToString());
                        throw;
                    }
                }
                else if (registered.HasReturnValue)
                {
                    run.Result = JsonSerializer.Serialize(result, options.SerializerOptions);
                }

                var elapsed = Stopwatch.GetElapsedTime(start);
                run.Status = JobStatus.Completed;
                run.CompletedAt = timeProvider.GetUtcNow();
                run.Progress = 1;
                await store.UpdateRunAsync(run);
                await AppendStatusEventAsync(run);

                SurefireMetrics.JobsExecuted.Add(1, tags);
                SurefireMetrics.JobDuration.Record(elapsed.TotalMilliseconds, tags);

                ctx.Result = run.Result is not null
                    ? JsonSerializer.Deserialize<object>(run.Result, options.SerializerOptions)
                    : null;
                await InvokeCallbacksAsync(options.OnSuccessCallbacks, sp, ctx, "OnSuccess", run.JobName);
                await InvokeCallbacksAsync(registered.OnSuccessCallbacks, sp, ctx, "OnSuccess", run.JobName);

                if (registered.Definition.IsContinuous)
                    await ScheduleContinuousRestartAsync(run, registered);
            }
            catch (OperationCanceledException) when (Volatile.Read(ref userCancelled) == 1)
            {
                // User-initiated cancel: flag was set atomically by cancel subscription
                run.Status = JobStatus.Cancelled;
                run.CompletedAt = timeProvider.GetUtcNow();
                await store.UpdateRunAsync(run);
                await AppendStatusEventAsync(run);
                SurefireMetrics.JobsCancelled.Add(1, tags);
            }
            catch (ChildRunCancelledException)
            {
                // A child run this job depends on was cancelled — treat as cancellation of this run too
                run.CancelledAt ??= timeProvider.GetUtcNow();
                run.Status = JobStatus.Cancelled;
                run.CompletedAt = timeProvider.GetUtcNow();
                await store.UpdateRunAsync(run);
                await AppendStatusEventAsync(run);
                SurefireMetrics.JobsCancelled.Add(1, tags);
            }
            catch (OperationCanceledException) when (hasTimeout && !shutdownToken.IsCancellationRequested)
            {
                // Timeout (not shutdown): mark as Failed with retry opportunity
                var elapsed = Stopwatch.GetElapsedTime(start);
                SurefireMetrics.JobsFailed.Add(1, tags);
                SurefireMetrics.JobDuration.Record(elapsed.TotalMilliseconds, tags);

                ctx.Exception = new TimeoutException("Job timed out");
                await HandleFailureWithRetryAsync(run, "Job timed out", registered, sp, ctx, tags);

                activity?.SetStatus(ActivityStatusCode.Error, "Job timed out");
            }
            catch (OperationCanceledException) when (shutdownToken.IsCancellationRequested)
            {
                // Shutdown: mark as Failed with retry opportunity
                var elapsed = Stopwatch.GetElapsedTime(start);
                SurefireMetrics.JobsFailed.Add(1, tags);
                SurefireMetrics.JobDuration.Record(elapsed.TotalMilliseconds, tags);

                ctx.Exception = new OperationCanceledException("Application shutdown");
                await HandleFailureWithRetryAsync(run, "Application shutdown", registered, sp, ctx, tags);
            }
            catch (Exception ex)
            {
                var elapsed = Stopwatch.GetElapsedTime(start);
                SurefireMetrics.JobsFailed.Add(1, tags);
                SurefireMetrics.JobDuration.Record(elapsed.TotalMilliseconds, tags);

                ctx.Exception = ex;
                await HandleFailureWithRetryAsync(run, ex.ToString(), registered, sp, ctx, tags);

                activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
            }
            finally
            {
                SurefireMetrics.ActiveRuns.Add(-1, tags);
                JobContext.Current.Value = null;
                try
                {
                    try { await loggerProvider.FlushAsync(); }
                    catch (Exception ex) { logger.LogWarning(ex, "Failed to flush logs for run {RunId}", run.Id); }

                    await notifications.PublishAsync(NotificationChannels.RunCompleted(run.Id), "");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Failed to publish completion notification for run {RunId}", run.Id);
                }
            }
        }
    }

    private async Task AppendStatusEventAsync(JobRun run, string? retryRunId = null)
    {
        string payload;
        if (retryRunId is not null)
            payload = JsonSerializer.Serialize(new { status = (int)run.Status, error = run.Error, retryRunId }, options.SerializerOptions);
        else if (run.Error is not null)
            payload = JsonSerializer.Serialize(new { status = (int)run.Status, error = run.Error }, options.SerializerOptions);
        else
            payload = JsonSerializer.Serialize(new { status = (int)run.Status }, options.SerializerOptions);

        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.Status,
            Payload = payload,
            CreatedAt = timeProvider.GetUtcNow()
        };
        await store.AppendEventAsync(evt);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), "");
    }

    private async Task HandleFailureWithRetryAsync(JobRun run, string error, RegisteredJob registered, IServiceProvider sp, JobContext ctx, TagList tags = default)
    {
        await InvokeCallbacksAsync(options.OnFailureCallbacks, sp, ctx, "OnFailure", run.JobName);
        await InvokeCallbacksAsync(registered.OnFailureCallbacks, sp, ctx, "OnFailure", run.JobName);

        var retryPolicy = registered.Definition.RetryPolicy;
        if (run.Attempt < retryPolicy.MaxAttempts)
        {
            await InvokeCallbacksAsync(options.OnRetryCallbacks, sp, ctx, "OnRetry", run.JobName);
            await InvokeCallbacksAsync(registered.OnRetryCallbacks, sp, ctx, "OnRetry", run.JobName);

            var delay = retryPolicy.GetDelay(run.Attempt);
            var now = timeProvider.GetUtcNow();

            // Create retry run first so retryRunId is available for the status event
            var retryRun = new JobRun
            {
                Id = Guid.CreateVersion7().ToString("N"),
                JobName = run.JobName,
                Arguments = run.Arguments,
                Attempt = run.Attempt + 1,
                CreatedAt = now,
                NotBefore = now + delay,
                Priority = run.Priority,
                RetryOfRunId = run.RetryOfRunId ?? run.Id
            };
            await store.CreateRunAsync(retryRun);
            await notifications.PublishAsync(NotificationChannels.RunCreated, retryRun.Id);
            SurefireMetrics.JobsRetried.Add(1, tags);

            run.Status = JobStatus.Failed;
            run.Error = error;
            run.CompletedAt = now;
            await store.UpdateRunAsync(run);
            await AppendStatusEventAsync(run, retryRun.Id);
            return;
        }

        run.Status = retryPolicy.MaxAttempts > 1 ? JobStatus.DeadLetter : JobStatus.Failed;
        run.Error = error;
        run.CompletedAt = timeProvider.GetUtcNow();
        await store.UpdateRunAsync(run);
        await AppendStatusEventAsync(run);

        if (run.Status == JobStatus.DeadLetter)
        {
            SurefireMetrics.JobsDeadLettered.Add(1, tags);
            await InvokeCallbacksAsync(options.OnDeadLetterCallbacks, sp, ctx, "OnDeadLetter", run.JobName);
            await InvokeCallbacksAsync(registered.OnDeadLetterCallbacks, sp, ctx, "OnDeadLetter", run.JobName);
        }

        if (registered.Definition.IsContinuous)
            await ScheduleContinuousRestartAsync(run, registered);
    }

    private async Task EnumerateOutputAsync(JobRun run, object result, CancellationToken ct)
    {
        // Discover IAsyncEnumerable<T> interface and item type at runtime
        var resultType = result.GetType();
        var asyncEnumInterface = resultType.GetInterfaces()
            .Concat([resultType])
            .First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>));
        var itemType = asyncEnumInterface.GetGenericArguments()[0];

        var method = typeof(JobExecutor)
            .GetMethod(nameof(EnumerateOutputCoreAsync), System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .MakeGenericMethod(itemType);

        await (Task)method.Invoke(this, [run, result, ct])!;
    }

    private async Task EnumerateOutputCoreAsync<T>(JobRun run, IAsyncEnumerable<T> stream, CancellationToken ct)
    {
        await foreach (var item in stream.WithCancellation(ct))
        {
            var payload = JsonSerializer.Serialize(item, options.SerializerOptions);
            var evt = new RunEvent
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = payload,
                CreatedAt = timeProvider.GetUtcNow()
            };
            await store.AppendEventAsync(evt, ct);
            await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), "", ct);
        }

        await AppendOutputCompleteAsync(run, error: null);
    }

    private async Task AppendOutputCompleteAsync(JobRun run, string? error)
    {
        var payload = error is not null
            ? JsonSerializer.Serialize(new { error }, options.SerializerOptions)
            : "{}";
        var evt = new RunEvent
        {
            RunId = run.Id,
            EventType = RunEventType.OutputComplete,
            Payload = payload,
            CreatedAt = timeProvider.GetUtcNow()
        };
        await store.AppendEventAsync(evt);
        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), "");
    }

    private async Task ScheduleContinuousRestartAsync(JobRun run, RegisteredJob registered)
    {
        var jobDef = await store.GetJobAsync(run.JobName);
        if (jobDef?.IsEnabled == false) return;

        var desired = registered.Definition.MaxConcurrency ?? 1;
        var pending = await store.GetRunsAsync(
            new RunFilter { JobName = run.JobName, ExactJobName = true, Status = JobStatus.Pending, Take = desired });
        if (pending.Items.Count >= desired) return;

        var now = timeProvider.GetUtcNow();
        var delay = run.Status == JobStatus.Completed
            ? TimeSpan.Zero
            : registered.Definition.RetryPolicy.InitialDelay;

        var restartRun = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = run.JobName,
            CreatedAt = now,
            NotBefore = now + delay,
            Priority = run.Priority,
        };
        await store.CreateRunAsync(restartRun);
        await notifications.PublishAsync(NotificationChannels.RunCreated, restartRun.Id);
        logger.LogDebug("Scheduled continuous restart for job {JobName}", run.JobName);
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
