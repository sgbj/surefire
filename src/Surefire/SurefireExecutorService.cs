using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireExecutorService(
    IJobStore store,
    INotificationProvider notifications,
    SurefireLogEventPump logEventPump,
    JobRegistry registry,
    ActiveRunTracker activeRunTracker,
    IServiceScopeFactory scopeFactory,
    SurefireOptions options,
    TimeProvider timeProvider,
    SurefireInstrumentation instrumentation,
    BatchCompletionHandler batchCompletionHandler,
    Backoff backoff,
    ILogger<SurefireExecutorService> logger) : BackgroundService
{
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(30);

    private readonly ConcurrentDictionary<string, Task> _activeTasks = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, CancellationTokenSource> _runCancellation =
        new(StringComparer.Ordinal);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var subscription = await notifications.SubscribeAsync(
            NotificationChannels.RunCreated,
            _ => ReleaseWakeupAsync(wakeup),
            stoppingToken);
        await using var availableSubscription = await notifications.SubscribeAsync(
            NotificationChannels.RunAvailable,
            _ => ReleaseWakeupAsync(wakeup),
            stoppingToken);

        var cancellationSweep = Task.Run(() => RunCancellationSweepAsync(stoppingToken), stoppingToken);

        try
        {
            var backoffAttempt = 0;
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (options.MaxNodeConcurrency is { } max && _activeTasks.Count >= max)
                    {
                        await WaitForWakeupAsync(wakeup, stoppingToken);
                        continue;
                    }

                    var claimStartedAt = Stopwatch.GetTimestamp();
                    var claimed = await store.ClaimRunAsync(
                        options.NodeName,
                        registry.GetJobNames(),
                        registry.GetQueueNames(),
                        stoppingToken);
                    instrumentation.RecordStoreOperation("claim",
                        Stopwatch.GetElapsedTime(claimStartedAt).TotalMilliseconds);

                    if (claimed is null)
                    {
                        await WaitForWakeupAsync(wakeup, stoppingToken);
                        continue;
                    }

                    instrumentation.RecordRunClaimed(claimed.JobName);

                    var executionTask = ExecuteClaimedRunAsync(claimed, stoppingToken);
                    _activeTasks[claimed.Id] = executionTask;
                    _ = executionTask.ContinueWith(_ =>
                        {
                            _activeTasks.TryRemove(claimed.Id, out var _);
                            _runCancellation.TryRemove(claimed.Id, out var _);
                            activeRunTracker.Remove(claimed.Id);
                            logEventPump.DropRunState(claimed.Id);
                        },
                        CancellationToken.None,
                        TaskContinuationOptions.None,
                        TaskScheduler.Default);
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

                    Log.ExecutorLoopFailed(logger, ex);
                    if (!store.IsTransientException(ex))
                    {
                        throw;
                    }

                    instrumentation.RecordStoreRetry("executor");
                    await Task.Delay(backoff.NextDelay(backoffAttempt++, BackoffInitial, BackoffMax), timeProvider,
                        stoppingToken);
                    continue;
                }

                backoffAttempt = 0;
            }

            await WaitForActiveRunsAsync();
        }
        finally
        {
            try
            {
                await cancellationSweep;
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    /// <summary>
    ///     Single consolidated polling fallback for missed cancellation notifications. Replaces the
    ///     per-run monitor task that the original implementation spawned for every active run, which
    ///     was O(active runs) store reads per polling interval. This is one batched read per interval
    ///     regardless of concurrency.
    /// </summary>
    private async Task RunCancellationSweepAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(options.PollingInterval, timeProvider);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                try
                {
                    var ids = activeRunTracker.Snapshot();
                    if (ids.Count == 0)
                    {
                        continue;
                    }

                    var stopped = await store.GetExternallyStoppedRunIdsAsync(ids, stoppingToken);
                    foreach (var id in stopped)
                    {
                        activeRunTracker.TryRequestCancel(id);
                    }
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    Log.CancellationSweepFailed(logger, ex);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
    }

    private async Task ExecuteClaimedRunAsync(JobRun run, CancellationToken stoppingToken)
    {
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _runCancellation[run.Id] = runCts;
        activeRunTracker.Add(run.Id, runCts);

        await using var cancellationSubscription = await notifications.SubscribeAsync(
            NotificationChannels.RunCancel(run.Id),
            _ =>
            {
                try
                {
                    runCts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }

                return Task.CompletedTask;
            },
            stoppingToken);
        var parentContext = ResolveParentActivityContext(run);
        using var activity = parentContext is { } ctx
            ? instrumentation.ActivitySource.StartActivity("surefire.run.execute", ActivityKind.Consumer, ctx)
            : instrumentation.ActivitySource.StartActivity("surefire.run.execute", ActivityKind.Consumer);
        if (activity is { })
        {
            await InitializeRunActivityAsync(activity, run, runCts.Token);
        }

        // Declared outside the try so the OCE catch can tell timeout apart from other cancellations.
        // Disposed explicitly in finally.
        CancellationTokenSource? timeoutCts = null;
        CancellationTokenSource? executionCts = null;
        IDisposable? jobContextScope = null;
        TimeSpan? configuredTimeout = null;

        try
        {
            if (!registry.TryGet(run.JobName, out var job))
            {
                using var deadLetterCts = CreateBestEffortWorkCts(stoppingToken);
                var deadLetterToken = deadLetterCts.Token;

                var noHandlerFailureEvent = batchCompletionHandler.CreateFailureEvent(
                    run,
                    RunFailureEnvelope.FromMessage(
                        run.Attempt,
                        timeProvider.GetUtcNow(),
                        "Executor",
                        "NoHandlerRegistered",
                        "No handler registered for this job.",
                        typeof(InvalidOperationException).FullName));

                if (options.CompiledOnDeadLetterCallbacks.Count > 0)
                {
                    await using var callbackScope = scopeFactory.CreateAsyncScope();
                    var callbackContext = new JobContext
                    {
                        RunId = run.Id,
                        RootRunId = run.RootRunId ?? run.Id,
                        JobName = run.JobName,
                        Attempt = run.Attempt,
                        BatchId = run.BatchId,
                        CancellationToken = deadLetterToken,
                        Store = store,
                        Notifications = notifications,
                        TimeProvider = timeProvider,
                        NodeName = run.NodeName ?? options.NodeName,
                        Exception = new InvalidOperationException("No handler registered for this job.")
                    };

                    await InvokeLifecycleCallbacksAsync(
                        options.CompiledOnDeadLetterCallbacks,
                        callbackContext,
                        callbackScope.ServiceProvider,
                        deadLetterToken);
                }

                var noHandlerTransition = RunStatusTransition.RunningToFailed(
                    run.Id,
                    run.Attempt,
                    timeProvider.GetUtcNow(),
                    run.NotBefore,
                    run.NodeName,
                    run.Progress,
                    "No handler registered for this job.",
                    run.Result,
                    run.StartedAt,
                    run.LastHeartbeatAt,
                    noHandlerFailureEvent is { } ? [noHandlerFailureEvent] : null);

                var dlResult = await FlushAndTransitionTerminalAsync(
                    run,
                    noHandlerTransition,
                    deadLetterToken);
                if (dlResult.Transitioned)
                {
                    instrumentation.RecordRunFailed(run.JobName, run.StartedAt, timeProvider.GetUtcNow());
                    await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id,
                        deadLetterToken);
                    await notifications.PublishAsync(NotificationChannels.RunAvailable, run.Id, deadLetterToken);
                    await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, deadLetterToken);
                    if (run.BatchId is { } noHandlerBatchId)
                    {
                        await notifications.PublishAsync(NotificationChannels.RunEvent(noHandlerBatchId), run.Id,
                            deadLetterToken);
                    }

                    if (dlResult.BatchCompletion is { } bc)
                    {
                        await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId,
                            deadLetterToken);
                    }
                }

                return;
            }

            // Timeout CTS fires independently of runCts so the OCE catch can distinguish
            // timeout from external cancellation. Linked with runCts so the handler observes
            // both via a single token. TimeProvider-aware so FakeTimeProvider drives it in tests.
            configuredTimeout = job.Definition.Timeout;
            if (configuredTimeout is { } timeout)
            {
                timeoutCts = new(timeout, timeProvider);
                executionCts = CancellationTokenSource.CreateLinkedTokenSource(runCts.Token, timeoutCts.Token);
            }

            var executionToken = executionCts?.Token ?? runCts.Token;

            var context = new JobContext
            {
                RunId = run.Id,
                RootRunId = run.RootRunId ?? run.Id,
                JobName = run.JobName,
                Attempt = run.Attempt,
                BatchId = run.BatchId,
                CancellationToken = executionToken,
                Store = store,
                Notifications = notifications,
                TimeProvider = timeProvider,
                NodeName = run.NodeName ?? options.NodeName
            };
            jobContextScope = JobContext.EnterScope(context);

            // Resolve handler/filter dependencies in a per-run scope (for scoped services).
            await using var scope = scopeFactory.CreateAsyncScope();

            var invocation = await ExecuteThroughFiltersAsync(job, run, context, scope.ServiceProvider, executionToken);
            context.Result = invocation.ResultObject;

            var completedAt = timeProvider.GetUtcNow();

            var completed = RunStatusTransition.RunningToSucceeded(
                run.Id,
                run.Attempt,
                completedAt,
                run.NotBefore,
                run.NodeName,
                1,
                invocation.ResultJson,
                null,
                run.StartedAt,
                completedAt);

            using var completionCts = CreateBestEffortWorkCts(stoppingToken);
            var completionToken = completionCts.Token;

            var transitionResult = await FlushAndTransitionTerminalAsync(run, completed, completionToken);
            if (transitionResult.Transitioned)
            {
                instrumentation.RecordRunCompleted(run.JobName, run.StartedAt, completedAt);
                await RunPostCompletionHousekeepingAsync(
                    job,
                    run,
                    context,
                    transitionResult,
                    scope.ServiceProvider,
                    completionToken);
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            await HandleRunFailureAsync(run, ex, stoppingToken);
        }
        catch (OperationCanceledException ex)
        {
            if (stoppingToken.IsCancellationRequested)
            {
                await HandleRunFailureAsync(
                    run,
                    new ShutdownInterruptedException(ex),
                    stoppingToken);
                return;
            }

            // A timeout is a failure, not a cancellation — the job exceeded its allowed
            // execution time. Route through HandleRunFailureAsync so it's retryable, has
            // an error message, and emits an AttemptFailure event visible on the dashboard.
            if (timeoutCts is { IsCancellationRequested: true })
            {
                await HandleRunFailureAsync(
                    run,
                    new TimeoutException(
                        $"Job execution timed out after {configuredTimeout}.", ex),
                    stoppingToken);
                return;
            }

            // If the run's own CancellationToken was not cancelled, this exception came from
            // post-handler bookkeeping (e.g. the best-effort completion token timing out),
            // not from a genuine cancellation request. Leave the run in Running — the
            // maintenance service will recover it via stale detection.
            if (!runCts.IsCancellationRequested)
            {
                Log.PostHandlerBookkeepingFailed(logger, ex, run.Id, run.JobName);
                try
                {
                    await batchCompletionHandler.AppendFailureEventAsync(
                        run,
                        RunFailureEnvelope.FromException(
                            run.Attempt,
                            timeProvider.GetUtcNow(),
                            ex,
                            "Executor",
                            "BookkeepingFailure"),
                        stoppingToken);
                }
                catch (Exception appendEx)
                {
                    Log.PostCompletionHousekeepingFailed(logger, appendEx, run.Id, run.JobName);
                }

                return;
            }

            using var bestEffortCts = CreateBestEffortWorkCts(stoppingToken);
            var bestEffortToken = bestEffortCts.Token;

            var cancelledAt = timeProvider.GetUtcNow();
            var cancelReason = "Cancelled by external request.";

            var cancelEvent = batchCompletionHandler.CreateFailureEvent(
                run,
                RunFailureEnvelope.FromMessage(
                    run.Attempt,
                    cancelledAt,
                    "Executor",
                    "RunCancelled",
                    cancelReason));

            await logEventPump.FlushRunAsync(run.Id, bestEffortToken);
            var cancelResult = await store.TryCancelRunAsync(
                run.Id,
                run.Attempt,
                cancelReason,
                cancelEvent is { } ? [cancelEvent] : null,
                bestEffortToken);

            if (cancelResult.Transitioned)
            {
                instrumentation.RecordRunCancelled(run.JobName, run.StartedAt, cancelledAt);
                await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id,
                    bestEffortToken);
                await notifications.PublishAsync(NotificationChannels.RunAvailable, run.Id, bestEffortToken);
                await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, bestEffortToken);
                if (run.BatchId is { } cancelledBatchId)
                {
                    await notifications.PublishAsync(NotificationChannels.RunEvent(cancelledBatchId), run.Id,
                        bestEffortToken);
                }

                if (cancelResult.BatchCompletion is { } bc)
                {
                    await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId,
                        bestEffortToken);
                }
            }
        }
        finally
        {
            jobContextScope?.Dispose();
            executionCts?.Dispose();
            timeoutCts?.Dispose();
            _runCancellation.TryRemove(run.Id, out _);
            activeRunTracker.Remove(run.Id);
            try
            {
                runCts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    private async Task HandleRunFailureAsync(JobRun run, Exception ex, CancellationToken stoppingToken)
    {
        using var bestEffortCts = CreateBestEffortWorkCts(stoppingToken);
        var bestEffortToken = bestEffortCts.Token;
        var isShutdownInterruption = ex is ShutdownInterruptedException;

        IReadOnlyList<CompiledCallback> jobDeadLetterCallbacks = [];
        if (registry.TryGet(run.JobName, out var job))
        {
            jobDeadLetterCallbacks = job.OnDeadLetterCallbacks;
            var canRetry = run.Attempt <= job.Definition.RetryPolicy.MaxRetries;
            if (canRetry)
            {
                var notBefore = timeProvider.GetUtcNow() + backoff.NextDelay(run.Attempt - 1,
                    job.Definition.RetryPolicy.InitialDelay, job.Definition.RetryPolicy.MaxDelay,
                    job.Definition.RetryPolicy.Jitter, job.Definition.RetryPolicy.BackoffType);

                if (isShutdownInterruption)
                {
                    Log.AttemptInterruptedByShutdownRetrying(
                        logger,
                        run.Attempt,
                        notBefore,
                        job.Definition.RetryPolicy.MaxRetries);
                }
                else
                {
                    logger.LogWarning(
                        ex,
                        "Attempt {Attempt} failed. Retrying at {NotBefore} (max retries: {MaxRetries}).",
                        run.Attempt,
                        notBefore,
                        job.Definition.RetryPolicy.MaxRetries);
                }

                await using var retryScope = scopeFactory.CreateAsyncScope();
                var retryContext = new JobContext
                {
                    RunId = run.Id,
                    RootRunId = run.RootRunId ?? run.Id,
                    JobName = run.JobName,
                    Attempt = run.Attempt,
                    BatchId = run.BatchId,
                    CancellationToken = bestEffortToken,
                    Store = store,
                    Notifications = notifications,
                    TimeProvider = timeProvider,
                    NodeName = run.NodeName ?? options.NodeName,
                    Exception = ex
                };

                await InvokeLifecycleCallbacksAsync(
                    [.. options.CompiledOnRetryCallbacks, .. job.OnRetryCallbacks],
                    retryContext,
                    retryScope.ServiceProvider,
                    bestEffortToken);

                var retryFailureEvent = batchCompletionHandler.CreateFailureEvent(
                    run,
                    RunFailureEnvelope.FromException(
                        run.Attempt,
                        timeProvider.GetUtcNow(),
                        ex,
                        "Executor",
                        GetFailureCode(ex)));

                // Retry path: do not set Reason — per-attempt failure detail lives on the
                // AttemptFailure event so dashboards and inspection can show the exception
                // per attempt. `run.Reason` is reserved for non-exception termination causes.
                var pending = RunStatusTransition.RunningToPending(
                    run.Id,
                    run.Attempt,
                    notBefore,
                    null,
                    run.Result,
                    events: retryFailureEvent is { } ? [retryFailureEvent] : null);

                if ((await store.TryTransitionRunAsync(pending, bestEffortToken)).Transitioned)
                {
                    await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id,
                        bestEffortToken);
                    await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id,
                        bestEffortToken);
                    return;
                }
            }
        }

        if (options.CompiledOnDeadLetterCallbacks.Count > 0 || jobDeadLetterCallbacks.Count > 0)
        {
            await using var callbackScope = scopeFactory.CreateAsyncScope();
            var callbackContext = new JobContext
            {
                RunId = run.Id,
                RootRunId = run.RootRunId ?? run.Id,
                JobName = run.JobName,
                Attempt = run.Attempt,
                BatchId = run.BatchId,
                CancellationToken = bestEffortToken,
                Store = store,
                Notifications = notifications,
                TimeProvider = timeProvider,
                NodeName = run.NodeName ?? options.NodeName,
                Exception = ex
            };

            await InvokeLifecycleCallbacksAsync(
                [.. options.CompiledOnDeadLetterCallbacks, .. jobDeadLetterCallbacks],
                callbackContext,
                callbackScope.ServiceProvider,
                bestEffortToken);
        }

        var failureEvent = batchCompletionHandler.CreateFailureEvent(
            run,
            RunFailureEnvelope.FromException(
                run.Attempt,
                timeProvider.GetUtcNow(),
                ex,
                "Executor",
                GetFailureCode(ex)));

        // Dead-letter path: Reason is only set when the run is being discarded for a
        // non-exception cause. Shutdown interruption is an environmental termination —
        // we surface it on Reason so the dashboard can distinguish it from retry
        // exhaustion. Ordinary failures (retry exhaustion) leave Reason null; the final
        // attempt's exception detail is captured by the AttemptFailure event.
        var deadLetter = RunStatusTransition.RunningToFailed(
            run.Id,
            run.Attempt,
            timeProvider.GetUtcNow(),
            run.NotBefore,
            run.NodeName,
            run.Progress,
            isShutdownInterruption ? "Shutdown interrupted mid-run." : null,
            run.Result,
            run.StartedAt,
            timeProvider.GetUtcNow(),
            failureEvent is { } ? [failureEvent] : null);

        if (isShutdownInterruption)
        {
            Log.ShutdownInterruptedRunFinalizingToDeadLetter(logger, run.Id, run.JobName, run.Attempt);
        }
        else
        {
            logger.LogError(ex,
                "Retries exhausted after attempt {Attempt}; flushing final events before moving run to dead letter.",
                run.Attempt);
        }

        var transitionResult = await FlushAndTransitionTerminalAsync(run, deadLetter, bestEffortToken);
        if (transitionResult.Transitioned)
        {
            instrumentation.RecordRunFailed(run.JobName, run.StartedAt, timeProvider.GetUtcNow());
            await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id, bestEffortToken);
            await notifications.PublishAsync(NotificationChannels.RunAvailable, run.Id, bestEffortToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, bestEffortToken);
            if (run.BatchId is { } deadLetterBatchId)
            {
                await notifications.PublishAsync(NotificationChannels.RunEvent(deadLetterBatchId), run.Id,
                    bestEffortToken);
            }

            if (transitionResult.BatchCompletion is { } bc)
            {
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId,
                    bestEffortToken);
            }
        }
    }

    private async Task InitializeRunActivityAsync(Activity activity, JobRun run, CancellationToken cancellationToken)
    {
        activity.SetTag("surefire.run.id", run.Id);
        activity.SetTag("surefire.run.job", run.JobName);
        activity.SetTag("surefire.run.attempt", run.Attempt);
        activity.SetTag("surefire.run.parent", run.ParentRunId);

        var traceId = activity.TraceId.ToString();
        var spanId = activity.SpanId.ToString();
        if (string.Equals(run.TraceId, traceId, StringComparison.Ordinal)
            && string.Equals(run.SpanId, spanId, StringComparison.Ordinal))
        {
            return;
        }

        try
        {
            await store.UpdateRunAsync(run with
            {
                TraceId = traceId,
                SpanId = spanId,
                LastHeartbeatAt = timeProvider.GetUtcNow()
            }, cancellationToken);
        }
        catch (Exception ex)
        {
            Log.FailedToPersistTraceContext(logger, ex, run.Id);
        }
    }

    private static ActivityContext? ResolveParentActivityContext(JobRun run)
        => TryParseActivityContext(run.ParentTraceId, run.ParentSpanId, out var context)
            ? context
            : null;

    private static bool TryParseActivityContext(string? traceId, string? spanId, out ActivityContext context)
    {
        if (string.IsNullOrWhiteSpace(traceId) || string.IsNullOrWhiteSpace(spanId))
        {
            context = default;
            return false;
        }

        try
        {
            var parsedTraceId = ActivityTraceId.CreateFromString(traceId.AsSpan());
            var parsedSpanId = ActivitySpanId.CreateFromString(spanId.AsSpan());
            context = new(parsedTraceId, parsedSpanId, ActivityTraceFlags.Recorded);
            return true;
        }
        catch (ArgumentOutOfRangeException)
        {
        }

        context = default;
        return false;
    }

    private async Task<RunTransitionResult> FlushAndTransitionTerminalAsync(JobRun run, RunStatusTransition transition,
        CancellationToken cancellationToken)
    {
        await logEventPump.FlushRunAsync(run.Id, cancellationToken);
        return await store.TryTransitionRunAsync(transition, cancellationToken);
    }


    private async Task<InvocationResult> ExecuteThroughFiltersAsync(RegisteredJob registration, JobRun run,
        JobContext context, IServiceProvider services, CancellationToken cancellationToken)
    {
        var allFilterTypes = new List<Type>(options.FilterTypes.Count + registration.FilterTypes.Count);
        allFilterTypes.AddRange(options.FilterTypes);
        allFilterTypes.AddRange(registration.FilterTypes);

        var invocation = InvocationResult.Empty;
        JobFilterDelegate pipeline = async _ =>
        {
            invocation = await InvokeHandlerAsync(registration, run, context, services, cancellationToken);
        };

        for (var i = allFilterTypes.Count - 1; i >= 0; i--)
        {
            var filter = (IJobFilter)ActivatorUtilities.GetServiceOrCreateInstance(services, allFilterTypes[i]);
            var next = pipeline;
            pipeline = ctx => filter.InvokeAsync(ctx, next);
        }

        await pipeline(context);
        return invocation;
    }

    private async Task<InvocationResult> InvokeHandlerAsync(RegisteredJob registration, JobRun run, JobContext context,
        IServiceProvider services, CancellationToken cancellationToken)
    {
        var metadata = registration.Metadata;
        var args = await BindParametersAsync(metadata, run, context, services, cancellationToken);

        var invocationResult = metadata.Invoke(args);

        var (hasResult, value) = await AwaitResultAsync(invocationResult, metadata);
        if (!hasResult)
        {
            return InvocationResult.Empty;
        }

        if (value is null)
        {
            return new(
                "null",
                null,
                null);
        }

        if (metadata.AsyncEnumerableElementType is { })
        {
            var items = await metadata.Materializer!(this, value, run, cancellationToken);
            var resultJson = $"[{string.Join(',', items)}]";
            return new(
                resultJson,
                value,
                items);
        }

        // Runtime guard: if a handler returns IAsyncEnumerable<T> through a type that the
        // metadata didn't detect at registration (e.g., untyped delegate), fail clearly
        // instead of letting JsonSerializer throw a confusing NotSupportedException.
        if (TypeHelpers.TryGetAsyncEnumerableElementType(value.GetType(), out _))
        {
            throw new InvalidOperationException(
                $"Job '{run.JobName}' returned an IAsyncEnumerable but the handler's return type was not detected as async-enumerable. " +
                "Ensure the handler's declared return type is IAsyncEnumerable<T>, not object or a non-generic wrapper.");
        }

        return new(
            JsonSerializer.Serialize(value, options.SerializerOptions),
            value,
            null);
    }

    private async Task<object?[]> BindParametersAsync(HandlerMetadata metadata, JobRun run,
        JobContext context, IServiceProvider services, CancellationToken cancellationToken)
    {
        var parameters = metadata.Parameters;
        using var doc = run.Arguments is null ? null : JsonDocument.Parse(run.Arguments);
        var root = doc?.RootElement;
        var declaredInputArguments = await GetDeclaredInputArgumentsAsync(run.Id, cancellationToken);
        var values = new object?[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            if (parameter.ParameterType == typeof(JobContext))
            {
                values[i] = context;
                continue;
            }

            if (parameter.ParameterType == typeof(CancellationToken))
            {
                values[i] = context.CancellationToken;
                continue;
            }

            var service = services.GetService(parameter.ParameterType);
            if (service is { })
            {
                values[i] = service;
                continue;
            }

            if (root is { ValueKind: JsonValueKind.Object }
                && TryGetJsonProperty(root.Value, parameter.Name!, out var property))
            {
                values[i] = BindJsonValue(property, parameter.ParameterType, options.SerializerOptions);
                continue;
            }

            if (metadata.StreamBinders[i] is { } binder)
            {
                if (TryGetDeclaredInputArgumentName(
                        parameters,
                        parameter,
                        root,
                        declaredInputArguments,
                        out var argumentName))
                {
                    values[i] = await binder(this, run.Id, argumentName, cancellationToken);
                    continue;
                }
            }

            if (root.HasValue && root.Value.ValueKind != JsonValueKind.Object)
            {
                values[i] = BindJsonValue(root.Value, parameter.ParameterType, options.SerializerOptions);
                continue;
            }

            if (parameter.HasDefaultValue)
            {
                values[i] = parameter.DefaultValue;
                continue;
            }

            throw new InvalidOperationException(
                $"Unable to bind parameter '{parameter.Name}' for job '{context.JobName}'.");
        }

        return values;
    }

    private async Task<HashSet<string>> GetDeclaredInputArgumentsAsync(string runId,
        CancellationToken cancellationToken)
    {
        var events = await store.GetEventsAsync(
            runId,
            0,
            [RunEventType.InputDeclared],
            0,
            cancellationToken: cancellationToken);

        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var @event in events)
        {
            var declaration = JsonSerializer.Deserialize(
                @event.Payload,
                SurefireJsonContext.Default.InputDeclarationEnvelope);
            if (declaration?.Arguments is null)
            {
                continue;
            }

            foreach (var argument in declaration.Arguments)
            {
                if (!string.IsNullOrWhiteSpace(argument))
                {
                    names.Add(argument);
                }
            }
        }

        return names;
    }

    private static bool TryGetDeclaredInputArgumentName(
        ParameterInfo[] parameters,
        ParameterInfo parameter,
        JsonElement? root,
        HashSet<string> declaredInputArguments,
        out string argumentName)
    {
        argumentName = string.Empty;
        if (parameter.Name is null)
        {
            return false;
        }

        if (declaredInputArguments.Contains(parameter.Name))
        {
            argumentName = parameter.Name;
            return true;
        }

        if (!root.HasValue
            && parameters.Length == 1
            && declaredInputArguments.Contains("$root"))
        {
            argumentName = "$root";
            return true;
        }

        return false;
    }

    internal IAsyncEnumerable<T> CreateInputStreamAsync<T>(string runId, string argumentName,
        CancellationToken cancellationToken) =>
        ReadInputStreamAsync<T>(runId, argumentName, cancellationToken);

    internal async IAsyncEnumerable<T> ReadInputStreamAsync<T>(string runId, string argumentName,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var subscription = await notifications.SubscribeAsync(
            NotificationChannels.RunInput(runId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        long sinceId = 0;
        var completed = false;

        while (!completed)
        {
            var events = await store.GetEventsAsync(runId, sinceId,
                [RunEventType.Input, RunEventType.InputComplete],
                0,
                cancellationToken: cancellationToken);

            foreach (var @event in events)
            {
                sinceId = @event.Id;
                var envelope = JsonSerializer.Deserialize(@event.Payload, SurefireJsonContext.Default.InputEnvelope);
                if (envelope is null
                    || !string.Equals(envelope.Argument, argumentName, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (@event.EventType == RunEventType.Input)
                {
                    if (envelope.Payload is { })
                    {
                        yield return JsonSerializer.Deserialize<T>(envelope.Payload, options.SerializerOptions)!;
                    }

                    continue;
                }

                if (!string.IsNullOrWhiteSpace(envelope.Error))
                {
                    throw new InvalidOperationException(
                        $"Input stream '{argumentName}' for run '{runId}' ended with error: {envelope.Error}");
                }

                completed = true;
                break;
            }

            if (!completed)
            {
                await wakeup.WaitAsync(options.PollingInterval, cancellationToken);
            }
        }
    }

    private static object? BindJsonValue(JsonElement value, Type targetType, JsonSerializerOptions serializerOptions)
    {
        if (targetType.IsGenericType
            && targetType.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            var elementType = targetType.GetGenericArguments()[0];
            var listType = typeof(List<>).MakeGenericType(elementType);
            var list = JsonSerializer.Deserialize(value.GetRawText(), listType, serializerOptions);
            var method = typeof(SurefireExecutorService)
                .GetMethod(nameof(ToAsyncEnumerable), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(elementType);
            return method.Invoke(null, [list]);
        }

        return JsonSerializer.Deserialize(value.GetRawText(), targetType, serializerOptions);
    }

    private static IAsyncEnumerable<T> ToAsyncEnumerable<T>(IEnumerable<T>? values)
    {
        return values is null ? EmptyAsync() : YieldAsync(values);

        static async IAsyncEnumerable<T> YieldAsync(IEnumerable<T> source)
        {
            foreach (var item in source)
            {
                yield return item;
                await Task.Yield();
            }
        }

        static async IAsyncEnumerable<T> EmptyAsync()
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private static bool TryGetJsonProperty(JsonElement root, string propertyName, out JsonElement value)
    {
        foreach (var property in root.EnumerateObject())
        {
            if (!string.Equals(property.Name, propertyName, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            value = property.Value;
            return true;
        }

        value = default;
        return false;
    }

    private static async Task<(bool HasResult, object? Value)> AwaitResultAsync(
        object? invocationResult,
        HandlerMetadata metadata)
    {
        if (!metadata.HasResult)
        {
            if (invocationResult is Task noResultTask)
            {
                await noResultTask;
            }
            else if (invocationResult is ValueTask noResultValueTask)
            {
                await noResultValueTask;
            }

            return (false, null);
        }

        switch (invocationResult)
        {
            case null:
                return (true, null);
            case Task task:
                await task;
                return (true, metadata.ExtractTaskResult!(task));
            case ValueTask:
                return (false, null);
            default:
            {
                if (metadata.AsTaskDelegate is { } asTask)
                {
                    var task = asTask(invocationResult);
                    await task;
                    return (true, metadata.ExtractTaskResult!(task));
                }

                return (true, invocationResult);
            }
        }
    }

    internal async Task<List<string>> MaterializeCoreAsync<T>(IAsyncEnumerable<T> stream,
        JobRun run,
        CancellationToken cancellationToken)
    {
        var items = new List<string>();
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            var payload = JsonSerializer.Serialize(item, options.SerializerOptions);
            items.Add(payload);
            await AppendOutputEventAsync(run, payload, cancellationToken);
        }

        await AppendOutputCompleteEventAsync(run, cancellationToken);

        return items;
    }

    private async Task AppendOutputEventAsync(JobRun run, string payload,
        CancellationToken cancellationToken)
    {
        await store.AppendEventsAsync(
        [
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = payload,
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = run.Attempt
            }
        ], cancellationToken);

        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
        if (run.BatchId is { } batchId1)
        {
            await notifications.PublishAsync(NotificationChannels.RunEvent(batchId1), run.Id, cancellationToken);
        }
    }

    private async Task AppendOutputCompleteEventAsync(JobRun run, CancellationToken cancellationToken)
    {
        await store.AppendEventsAsync(
        [
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = run.Attempt
            }
        ], cancellationToken);

        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
        if (run.BatchId is { } batchId2)
        {
            await notifications.PublishAsync(NotificationChannels.RunEvent(batchId2), run.Id, cancellationToken);
        }
    }

    private async Task TryCreateContinuousRunAsync(RegisteredJob job, JobRun completedRun,
        CancellationToken cancellationToken)
    {
        if (!job.Definition.IsContinuous || completedRun.ParentRunId is { })
        {
            return;
        }

        var now = timeProvider.GetUtcNow();
        var nextRun = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = completedRun.JobName,
            Status = JobStatus.Pending,
            Arguments = completedRun.Arguments,
            CreatedAt = now,
            NotBefore = now,
            Priority = job.Definition.Priority,
            QueuePriority = 0,
            Progress = 0,
            DeduplicationId = BuildContinuousDeduplicationId(completedRun.Id),
            Attempt = 0
        };

        var created = await store.TryCreateRunAsync(
            nextRun,
            job.Definition.MaxConcurrency ?? 1,
            cancellationToken: cancellationToken);
        if (!created)
        {
            return;
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, nextRun.Id, cancellationToken);
    }

    private async Task RunPostCompletionHousekeepingAsync(RegisteredJob job, JobRun run, JobContext context,
        RunTransitionResult transitionResult, IServiceProvider services, CancellationToken cancellationToken)
    {
        try
        {
            await InvokeLifecycleCallbacksAsync(
                [.. options.CompiledOnSuccessCallbacks, .. job.OnSuccessCallbacks],
                context,
                services,
                cancellationToken);

            await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunAvailable, run.Id, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, cancellationToken);
            if (run.BatchId is { } completedBatchId)
            {
                await notifications.PublishAsync(NotificationChannels.RunEvent(completedBatchId), run.Id,
                    cancellationToken);
            }

            if (transitionResult.BatchCompletion is { } bc)
            {
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(bc.BatchId), bc.BatchId,
                    cancellationToken);
            }

            await TryCreateContinuousRunAsync(job, run, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Log.PostCompletionHousekeepingFailed(logger, ex, run.Id, run.JobName);
        }
    }

    private static string BuildContinuousDeduplicationId(string completedRunId)
        => $"continuous:{completedRunId}";

    private async Task InvokeLifecycleCallbacksAsync(IReadOnlyList<CompiledCallback> callbacks, JobContext context,
        IServiceProvider services, CancellationToken cancellationToken)
    {
        foreach (var callback in callbacks)
        {
            try
            {
                await callback.InvokeAsync(context, services, cancellationToken);
            }
            catch (Exception ex)
            {
                Log.LifecycleCallbackFailed(logger, ex, context.JobName);
            }
        }
    }

    private static string GetFailureCode(Exception exception) => exception switch
    {
        ShutdownInterruptedException => "ShutdownInterrupted",
        OperationCanceledException => "OperationCanceled",
        _ => "Exception"
    };

    private static Task ReleaseWakeupAsync(SemaphoreSlim wakeup) => WakeupSignal.ReleaseAsync(wakeup);

    private async Task WaitForWakeupAsync(SemaphoreSlim wakeup, CancellationToken cancellationToken)
    {
        await WakeupSignal.WaitAsync(wakeup, options.PollingInterval, cancellationToken);
    }

    private CancellationTokenSource CreateBestEffortWorkCts(CancellationToken stoppingToken)
    {
        if (stoppingToken.IsCancellationRequested)
        {
            return new(options.ShutdownTimeout);
        }

        var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        cts.CancelAfter(options.ShutdownTimeout);
        return cts;
    }

    private async Task WaitForActiveRunsAsync()
    {
        if (_activeTasks.Count == 0)
        {
            return;
        }

        // Signal all active runs to cancel, then wait up to ShutdownTimeout for them
        // to complete cleanup (state transitions, failure events, notifications).
        foreach (var cts in _runCancellation.Values)
        {
            try
            {
                cts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }

        using var timeoutCts = new CancellationTokenSource(options.ShutdownTimeout);
        try
        {
            await Task.WhenAll(_activeTasks.Values).WaitAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1101, Level = LogLevel.Error, Message = "Executor loop failed.")]
        public static partial void ExecutorLoopFailed(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 1102, Level = LogLevel.Warning,
            Message = "Failed to persist trace context for run '{RunId}'.")]
        public static partial void FailedToPersistTraceContext(ILogger logger, Exception exception, string runId);

        [LoggerMessage(EventId = 1103, Level = LogLevel.Warning,
            Message = "Cancellation sweep failed.")]
        public static partial void CancellationSweepFailed(ILogger logger, Exception exception);

        [LoggerMessage(EventId = 1104, Level = LogLevel.Warning,
            Message = "Lifecycle callback failed for job '{JobName}'.")]
        public static partial void LifecycleCallbackFailed(ILogger logger, Exception exception, string jobName);

        [LoggerMessage(EventId = 1106, Level = LogLevel.Warning,
            Message = "Post-completion housekeeping failed for run '{RunId}' (job '{JobName}').")]
        public static partial void PostCompletionHousekeepingFailed(ILogger logger, Exception exception, string runId,
            string jobName);

        [LoggerMessage(EventId = 1107, Level = LogLevel.Warning,
            Message =
                "Attempt {Attempt} interrupted by application shutdown. Retrying at {NotBefore} (max retries: {MaxRetries}).")]
        public static partial void AttemptInterruptedByShutdownRetrying(ILogger logger, int attempt,
            DateTimeOffset notBefore, int maxRetries);

        [LoggerMessage(EventId = 1108, Level = LogLevel.Warning,
            Message =
                "Run '{RunId}' (job '{JobName}') was interrupted by application shutdown; flushing final events before moving it to dead letter after attempt {Attempt}.")]
        public static partial void ShutdownInterruptedRunFinalizingToDeadLetter(ILogger logger, string runId,
            string jobName, int attempt);

        [LoggerMessage(EventId = 1109, Level = LogLevel.Error,
            Message =
                "Post-handler bookkeeping failed for run '{RunId}' (job '{JobName}'). The run will remain in Running status for maintenance recovery.")]
        public static partial void PostHandlerBookkeepingFailed(ILogger logger, Exception exception, string runId,
            string jobName);
    }

    private sealed record InvocationResult(
        string? ResultJson,
        object? ResultObject,
        IReadOnlyList<string>? OutputItems)
    {
        public static InvocationResult Empty { get; } = new(null, null, null);
    }
}

internal sealed class ShutdownInterruptedException(Exception innerException)
    : Exception("Run interrupted because the application is shutting down.", innerException);