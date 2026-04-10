using System.Collections.Concurrent;
using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Surefire;

internal sealed partial class SurefireExecutorService(
    IJobStore store,
    INotificationProvider notifications,
    JobRegistry registry,
    ActiveRunTracker activeRunTracker,
    IServiceScopeFactory scopeFactory,
    SurefireOptions options,
    TimeProvider timeProvider,
    SurefireInstrumentation instrumentation,
    BatchCompletionHandler batchCompletionHandler,
    ILogger<SurefireExecutorService> logger) : BackgroundService
{
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

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (options.MaxNodeConcurrency is { } max && _activeTasks.Count >= max)
                {
                    await WaitForWakeupAsync(wakeup, stoppingToken);
                    continue;
                }

                var sw = Stopwatch.StartNew();
                var claimed = await store.ClaimRunAsync(
                    options.NodeName,
                    registry.GetJobNames(),
                    registry.GetQueueNames(),
                    stoppingToken);
                instrumentation.RecordStoreOperation("claim", sw.Elapsed.TotalMilliseconds);

                if (claimed is null)
                {
                    await WaitForWakeupAsync(wakeup, stoppingToken);
                    continue;
                }

                instrumentation.RecordRunClaimed(claimed.JobName);

                var executionTask = ExecuteClaimedRunAsync(claimed, stoppingToken);
                _activeTasks[claimed.Id] = executionTask;
                activeRunTracker.Add(claimed.Id);
                _ = executionTask.ContinueWith(_ =>
                    {
                        _activeTasks.TryRemove(claimed.Id, out var ignored);
                        activeRunTracker.Remove(claimed.Id);
                        ReleaseWakeupAsync(wakeup);
                    },
                    CancellationToken.None,
                    TaskContinuationOptions.ExecuteSynchronously,
                    TaskScheduler.Default);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Log.ExecutorLoopFailed(logger, ex);
                await Task.Delay(TimeSpan.FromSeconds(1), stoppingToken);
            }
        }

        await WaitForActiveRunsAsync();
    }

    private async Task ExecuteClaimedRunAsync(JobRun run, CancellationToken stoppingToken)
    {
        using var runCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
        _runCancellation[run.Id] = runCts;
        var monitorTask = MonitorExternalRunStateAsync(run.Id, runCts, stoppingToken);

        await using var cancellationSubscription = await notifications.SubscribeAsync(
            NotificationChannels.RunCancel(run.Id),
            _ =>
            {
                runCts.Cancel();
                return Task.CompletedTask;
            },
            stoppingToken);
        using var activity = await StartRunActivityAsync(run, runCts.Token);
        var context = new JobContext
        {
            RunId = run.Id,
            RootRunId = run.RootRunId ?? run.Id,
            JobName = run.JobName,
            Attempt = run.Attempt,
            BatchRunId = run.BatchId,
            CancellationToken = runCts.Token,
            Store = store,
            Notifications = notifications,
            TimeProvider = timeProvider,
            NodeName = run.NodeName ?? options.NodeName
        };
        using var jobContextScope = JobContext.EnterScope(context);

        try
        {
            if (!registry.TryGet(run.JobName, out var job))
            {
                if (await TryTransitionToDeadLetterAsync(run, "No handler registered for this job.", stoppingToken))
                {
                    await batchCompletionHandler.AppendFailureEventAsync(
                        run,
                        RunFailureEnvelope.FromMessage(
                            run.Attempt,
                            timeProvider.GetUtcNow(),
                            "Executor",
                            "NoHandlerRegistered",
                            "No handler registered for this job.",
                            typeof(InvalidOperationException).FullName),
                        stoppingToken);
                    instrumentation.RecordRunFailed(run.JobName, run.StartedAt, timeProvider.GetUtcNow());
                    await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id, stoppingToken);
                    await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, stoppingToken);
                    await batchCompletionHandler.MaybeCompleteBatchAsync(run.BatchId, run.Id, JobStatus.Failed, "Executor", stoppingToken);
                }

                return;
            }

            if (job.Definition.Timeout is { } timeout)
            {
                runCts.CancelAfter(timeout);
            }

            // Resolve handler/filter dependencies in a per-run scope (for scoped services).
            await using var scope = scopeFactory.CreateAsyncScope();

            var invocation = await ExecuteThroughFiltersAsync(job, run, context, scope.ServiceProvider, runCts.Token);
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

            if (await store.TryTransitionRunAsync(completed, runCts.Token))
            {
                instrumentation.RecordRunCompleted(run.JobName, run.StartedAt, completedAt);
                await RunPostCompletionHousekeepingAsync(
                    job,
                    run,
                    context,
                    scope.ServiceProvider,
                    stoppingToken);
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

            using var bestEffortCts = CreateBestEffortWorkCts(stoppingToken);
            var bestEffortToken = bestEffortCts.Token;

            var cancelledAt = timeProvider.GetUtcNow();
            var cancelled = RunStatusTransition.ToCancelled(
                JobStatus.Running,
                run.Id,
                run.Attempt,
                cancelledAt,
                cancelledAt,
                run.NotBefore,
                run.NodeName,
                run.Progress,
                null,
                run.Result,
                run.StartedAt,
                cancelledAt);

            if (await store.TryTransitionRunAsync(cancelled, bestEffortToken))
            {
                instrumentation.RecordRunCancelled(run.JobName, run.StartedAt, cancelledAt);
                await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id,
                    bestEffortToken);
                await batchCompletionHandler.MaybeCompleteBatchAsync(run.BatchId, run.Id, JobStatus.Cancelled, "Executor", bestEffortToken);
            }
        }
        finally
        {
            _runCancellation.TryRemove(run.Id, out _);
            runCts.Cancel();
            await monitorTask;
        }
    }

    private async Task HandleRunFailureAsync(JobRun run, Exception ex, CancellationToken stoppingToken)
    {
        using var bestEffortCts = CreateBestEffortWorkCts(stoppingToken);
        var bestEffortToken = bestEffortCts.Token;
        var isShutdownInterruption = ex is ShutdownInterruptedException;

        await batchCompletionHandler.AppendFailureEventAsync(
            run,
            RunFailureEnvelope.FromException(
                run.Attempt,
                timeProvider.GetUtcNow(),
                ex,
                "Executor",
                GetFailureCode(ex)),
            bestEffortToken);

        if (registry.TryGet(run.JobName, out var job))
        {
            var canRetry = run.Attempt <= job.Definition.RetryPolicy.MaxRetries;
            var attemptError = BuildDeadLetterError(ex);
            if (canRetry)
            {
                var notBefore = timeProvider.GetUtcNow() + job.Definition.RetryPolicy.GetDelay(run.Attempt);
                var retrying = RunStatusTransition.RunningToRetrying(
                    run.Id,
                    run.Attempt,
                    notBefore,
                    run.NodeName,
                    run.Progress,
                    attemptError,
                    run.Result,
                    timeProvider.GetUtcNow());

                if (await store.TryTransitionRunAsync(retrying, bestEffortToken))
                {
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

                    if (ex is not OperationCanceledException)
                    {
                        await using var retryScope = scopeFactory.CreateAsyncScope();
                        var retryContext = new JobContext
                        {
                            RunId = run.Id,
                            RootRunId = run.RootRunId ?? run.Id,
                            JobName = run.JobName,
                            Attempt = run.Attempt,
                            BatchRunId = run.BatchId,
                            CancellationToken = bestEffortToken,
                            Store = store,
                            Notifications = notifications,
                            TimeProvider = timeProvider,
                            NodeName = run.NodeName ?? options.NodeName,
                            Exception = ex
                        };

                        await InvokeLifecycleCallbacksAsync(
                            [.. options.OnRetryCallbacks, .. job.OnRetryCallbacks],
                            retryContext,
                            retryScope.ServiceProvider,
                            bestEffortToken);
                    }

                    var pending = RunStatusTransition.RetryingToPending(
                        run.Id,
                        run.Attempt,
                        notBefore,
                        null,
                        run.Progress,
                        attemptError,
                        run.Result);

                    if (await store.TryTransitionRunAsync(pending, bestEffortToken))
                    {
                        await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id,
                            bestEffortToken);
                        await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id,
                            bestEffortToken);
                        return;
                    }
                }
            }
        }

        if (registry.TryGet(run.JobName, out var deadLetterJob)
            && ex is not OperationCanceledException)
        {
            await using var callbackScope = scopeFactory.CreateAsyncScope();
            var callbackContext = new JobContext
            {
                RunId = run.Id,
                RootRunId = run.RootRunId ?? run.Id,
                JobName = run.JobName,
                Attempt = run.Attempt,
                BatchRunId = run.BatchId,
                CancellationToken = bestEffortToken,
                Store = store,
                Notifications = notifications,
                TimeProvider = timeProvider,
                NodeName = run.NodeName ?? options.NodeName,
                Exception = ex
            };

            await InvokeLifecycleCallbacksAsync(
                [.. options.OnDeadLetterCallbacks, .. deadLetterJob.OnDeadLetterCallbacks],
                callbackContext,
                callbackScope.ServiceProvider,
                bestEffortToken);
        }

        if (await TryTransitionToDeadLetterAsync(run, BuildDeadLetterError(ex), bestEffortToken))
        {
            if (isShutdownInterruption)
            {
                Log.ShutdownInterruptedRunMovedToDeadLetter(logger, run.Id, run.JobName, run.Attempt);
            }
            else
            {
                logger.LogError(ex, "Run moved to dead letter after attempt {Attempt}.", run.Attempt);
            }

            instrumentation.RecordRunFailed(run.JobName, run.StartedAt, timeProvider.GetUtcNow());
            await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id,
                bestEffortToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, bestEffortToken);
            await batchCompletionHandler.MaybeCompleteBatchAsync(run.BatchId, run.Id, JobStatus.Failed, "Executor", bestEffortToken);
        }
    }

    private async Task<Activity?> StartRunActivityAsync(JobRun run, CancellationToken cancellationToken)
    {
        var parent = await ResolveParentActivityContextAsync(run, cancellationToken);
        var activity = parent is { } parentContext
            ? instrumentation.ActivitySource.StartActivity("surefire.run.execute", ActivityKind.Consumer, parentContext)
            : instrumentation.ActivitySource.StartActivity("surefire.run.execute", ActivityKind.Consumer);

        if (activity is null)
        {
            return null;
        }

        activity.SetTag("surefire.run.id", run.Id);
        activity.SetTag("surefire.run.job", run.JobName);
        activity.SetTag("surefire.run.attempt", run.Attempt);
        activity.SetTag("surefire.run.parent", run.ParentRunId);

        var traceId = activity.TraceId.ToString();
        var spanId = activity.SpanId.ToString();
        if (!string.Equals(run.TraceId, traceId, StringComparison.Ordinal)
            || !string.Equals(run.SpanId, spanId, StringComparison.Ordinal))
        {
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

        return activity;
    }

    private async Task<ActivityContext?> ResolveParentActivityContextAsync(JobRun run,
        CancellationToken cancellationToken)
    {
        if (TryParseActivityContext(run.TraceId, run.SpanId, out var runContext))
        {
            return runContext;
        }

        if (run.ParentRunId is null)
        {
            return null;
        }

        var parentRun = await store.GetRunAsync(run.ParentRunId, cancellationToken);
        if (parentRun is null)
        {
            return null;
        }

        return TryParseActivityContext(parentRun.TraceId, parentRun.SpanId, out var parentContext)
            ? parentContext
            : null;
    }

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

    private async Task<bool> TryTransitionToDeadLetterAsync(JobRun run, string error,
        CancellationToken cancellationToken)
    {
        var deadLetter = RunStatusTransition.RunningToFailed(
            run.Id,
            run.Attempt,
            timeProvider.GetUtcNow(),
            run.NotBefore,
            run.NodeName,
            run.Progress,
            error,
            run.Result,
            run.StartedAt,
            timeProvider.GetUtcNow());

        return await store.TryTransitionRunAsync(deadLetter, cancellationToken);
    }

    private static string BuildDeadLetterError(Exception exception) => exception.ToString();

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

        if (metadata.AsyncEnumerableElementType is not null)
        {
            var items = await metadata.Materializer!(this, value, run, cancellationToken);
            var resultJson = $"[{string.Join(',', items)}]";
            return new(
                resultJson,
                value,
                items);
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
            attempt: 0,
            cancellationToken: cancellationToken);

        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var @event in events)
        {
            var declaration = JsonSerializer.Deserialize<InputDeclarationEnvelope>(
                @event.Payload,
                options.SerializerOptions);
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
                attempt: 0,
                cancellationToken: cancellationToken);

            foreach (var @event in events)
            {
                sinceId = @event.Id;
                var envelope = JsonSerializer.Deserialize<InputEnvelope>(@event.Payload, options.SerializerOptions);
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
                await noResultTask;
            else if (invocationResult is ValueTask noResultValueTask)
                await noResultValueTask;
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

    private async Task MonitorExternalRunStateAsync(string runId, CancellationTokenSource runCts,
        CancellationToken stoppingToken)
    {
        CancellationToken runToken;
        try
        {
            runToken = runCts.Token;
        }
        catch (ObjectDisposedException)
        {
            return;
        }

        using var wakeup = new SemaphoreSlim(0, 1);
        await using var eventSub = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(runId),
            _ => ReleaseWakeupAsync(wakeup),
            stoppingToken);
        await using var completionSub = await notifications.SubscribeAsync(
            NotificationChannels.RunTerminated(runId),
            _ => ReleaseWakeupAsync(wakeup),
            stoppingToken);

        while (!runToken.IsCancellationRequested && !stoppingToken.IsCancellationRequested)
        {
            try
            {
                await wakeup.WaitAsync(options.PollingInterval, runToken);
                if (runToken.IsCancellationRequested)
                {
                    return;
                }

                var current = await store.GetRunAsync(runId, stoppingToken);
                if (current is null || current.Status.IsTerminal)
                {
                    try
                    {
                        runCts.Cancel();
                    }
                    catch (ObjectDisposedException)
                    {
                        // Cancellation source can be disposed during teardown.
                    }

                    return;
                }
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch (Exception ex)
            {
                Log.FailedToMonitorRunState(logger, ex, runId);
            }
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
        IServiceProvider services, CancellationToken stoppingToken)
    {
        try
        {
            await InvokeLifecycleCallbacksAsync(
                [.. options.OnSuccessCallbacks, .. job.OnSuccessCallbacks],
                context,
                services,
                stoppingToken);

            await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id, stoppingToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, stoppingToken);
            await batchCompletionHandler.MaybeCompleteBatchAsync(run.BatchId, run.Id, JobStatus.Succeeded, "Executor", stoppingToken);
            await TryCreateContinuousRunAsync(job, run, stoppingToken);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            Log.PostCompletionHousekeepingFailed(logger, ex, run.Id, run.JobName);
        }
    }

    private static string BuildContinuousDeduplicationId(string completedRunId)
        => $"continuous:{completedRunId}";

    private async Task InvokeLifecycleCallbacksAsync(IReadOnlyList<Delegate> callbacks, JobContext context,
        IServiceProvider services, CancellationToken cancellationToken)
    {
        foreach (var callback in callbacks)
        {
            try
            {
                await InvokeCallbackAsync(callback, context, services, cancellationToken);
            }
            catch (Exception ex)
            {
                Log.LifecycleCallbackFailed(logger, ex, context.JobName);
            }
        }
    }

    private async Task AppendAttemptFailureEventAsync(JobRun run, Exception exception,
        CancellationToken cancellationToken)
        => await batchCompletionHandler.AppendFailureEventAsync(
            run,
            RunFailureEnvelope.FromException(
                run.Attempt,
                timeProvider.GetUtcNow(),
                exception,
                "Executor",
                GetFailureCode(exception)),
            cancellationToken);

    private static string GetFailureCode(Exception exception) => exception switch
    {
        ShutdownInterruptedException => "ShutdownInterrupted",
        OperationCanceledException => "OperationCanceled",
        _ => "Exception"
    };

    private static async Task InvokeCallbackAsync(Delegate callback, JobContext context,
        IServiceProvider services, CancellationToken cancellationToken)
    {
        var parameters = callback.Method.GetParameters();
        var args = new object?[parameters.Length];

        for (var i = 0; i < parameters.Length; i++)
        {
            var parameter = parameters[i];
            if (parameter.ParameterType == typeof(JobContext))
            {
                args[i] = context;
                continue;
            }

            if (parameter.ParameterType == typeof(CancellationToken))
            {
                args[i] = cancellationToken;
                continue;
            }

            if (context.Exception is { } && parameter.ParameterType.IsInstanceOfType(context.Exception))
            {
                args[i] = context.Exception;
                continue;
            }

            if (context.Result is { } && parameter.ParameterType.IsInstanceOfType(context.Result))
            {
                args[i] = context.Result;
                continue;
            }

            var service = services.GetService(parameter.ParameterType);
            if (service is { })
            {
                args[i] = service;
                continue;
            }

            if (parameter.HasDefaultValue)
            {
                args[i] = parameter.DefaultValue;
                continue;
            }

            throw new InvalidOperationException(
                $"Unable to bind callback parameter '{parameter.Name}' for job '{context.JobName}'.");
        }

        try
        {
            var returned = callback.DynamicInvoke(args);
            if (returned is Task task)
            {
                await task;
                return;
            }

            if (returned is ValueTask valueTask)
            {
                await valueTask;
                return;
            }

            if (returned is { })
            {
                var type = returned.GetType();
                if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ValueTask<>))
                {
                    var asTaskMethod = type.GetMethod("AsTask")!;
                    var convertedTask = (Task)asTaskMethod.Invoke(returned, null)!;
                    await convertedTask;
                }
            }
        }
        catch (TargetInvocationException ex) when (ex.InnerException is not null)
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            throw;
        }
    }

    private static Task ReleaseWakeupAsync(SemaphoreSlim wakeup)
    {
        try
        {
            wakeup.Release();
        }
        catch (SemaphoreFullException)
        {
        }
        catch (ObjectDisposedException)
        {
            // Notification callbacks can race with monitor teardown; no wakeup is needed after disposal.
        }

        return Task.CompletedTask;
    }

    private async Task WaitForWakeupAsync(SemaphoreSlim wakeup, CancellationToken cancellationToken)
    {
        await wakeup.WaitAsync(options.PollingInterval, cancellationToken);
    }

    private CancellationTokenSource CreateBestEffortWorkCts(CancellationToken stoppingToken)
    {
        if (stoppingToken.IsCancellationRequested)
        {
            return new CancellationTokenSource(options.ShutdownTimeout);
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
            cts.Cancel();
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
            Message = "Failed to monitor run state for run '{RunId}'.")]
        public static partial void FailedToMonitorRunState(ILogger logger, Exception exception, string runId);

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
                "Run '{RunId}' (job '{JobName}') interrupted by application shutdown moved to dead letter after attempt {Attempt}.")]
        public static partial void ShutdownInterruptedRunMovedToDeadLetter(ILogger logger, string runId,
            string jobName, int attempt);
    }

    private sealed record InvocationResult(
        string? ResultJson,
        object? ResultObject,
        IReadOnlyList<string>? OutputItems)
    {
        public static InvocationResult Empty { get; } = new(null, null, null);
    }

    private sealed class ShutdownInterruptedException(Exception innerException)
        : Exception("Run interrupted because the application is shutting down.", innerException);
}