using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
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
    LoopHealthTracker loopHealth,
    BatchCompletionHandler batchCompletionHandler,
    BatchedEventWriter eventWriter,
    RunCancellationCoordinator runCancellation,
    Backoff backoff,
    ILogger<SurefireExecutorService> logger) : BackgroundService, IJobRunPipe
{
    internal const string LoopName = "executor";

    // Bounds per-claim latency, memory, and lock duration on the claim path.
    private const int MaxBatchClaim = 32;
    private static readonly TimeSpan BackoffInitial = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan BackoffMax = TimeSpan.FromSeconds(30);

    // Deadlocks/serialization failures clear within milliseconds of the victim rollback, so
    // transient-retry runs tighter than the loop-level backoff. Bounded only by caller cancellation.
    private static readonly TimeSpan TransientRetryInitial = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan TransientRetryMax = TimeSpan.FromSeconds(1);

    private readonly ConcurrentDictionary<string, Task> _activeTasks = new(StringComparer.Ordinal);

    private readonly ConcurrentDictionary<string, CancellationTokenSource> _runCancellation =
        new(StringComparer.Ordinal);

    /// <summary>
    ///     AOT-safe implementation of <see cref="IJobRunPipe.ReadInputStreamAsync{T}" /> backing the
    ///     descriptor path. Decodes each <see cref="RunEventType.Input" /> event with the supplied
    ///     <see cref="JsonTypeInfo{T}" /> rather than the reflection-based resolver.
    /// </summary>
    async IAsyncEnumerable<T> IJobRunPipe.ReadInputStreamAsync<T>(JobRun run, string argumentName,
        JsonTypeInfo<T> typeInfo, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in ReadInputStreamCoreAsync<T>(run.Id, argumentName,
                           payload => JsonSerializer.Deserialize(payload, typeInfo)!, cancellationToken))
        {
            yield return item;
        }
    }

    /// <summary>
    ///     AOT-safe implementation of <see cref="IJobRunPipe.WriteOutputStreamAsync{T}" /> backing
    ///     the descriptor path. Serializes each element with the supplied
    ///     <see cref="JsonTypeInfo{T}" /> and emits an <see cref="RunEventType.Output" /> event per
    ///     item, followed by an <see cref="RunEventType.OutputComplete" /> event.
    /// </summary>
    async Task<IReadOnlyList<string>> IJobRunPipe.WriteOutputStreamAsync<T>(IAsyncEnumerable<T> stream,
        JsonTypeInfo<T> typeInfo, JobRun run, CancellationToken cancellationToken)
    {
        return await WriteOutputStreamCoreAsync(stream,
            value => JsonSerializer.Serialize(value, typeInfo), run, cancellationToken);
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        // Owned here (not as a separate IHostedService) so writer drain is sequenced with the
        // executor loop: starts before the first claim, stops after every handler has stopped
        // enqueueing. This holds regardless of HostOptions.ServicesStopConcurrently.
        await eventWriter.StartAsync(cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    public override async Task StopAsync(CancellationToken cancellationToken)
    {
        // base.StopAsync waits for _activeTasks to drain. Only then is it safe to stop the writer:
        // every accepted event is now either persisted or explicitly failed.
        await base.StopAsync(cancellationToken);
        await eventWriter.StopAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        loopHealth.Register(LoopName, options.PollingInterval);
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
                    var capacity = options.MaxNodeConcurrency is { } max
                        ? max - _activeTasks.Count
                        : MaxBatchClaim;

                    if (capacity <= 0)
                    {
                        await WaitForWakeupAsync(wakeup, stoppingToken);
                        continue;
                    }

                    var batchSize = Math.Min(capacity, MaxBatchClaim);

                    var claimStartedAt = Stopwatch.GetTimestamp();
                    var claimed = await store.ClaimRunsAsync(
                        options.NodeName,
                        registry.GetJobNames(),
                        registry.GetQueueNames(),
                        batchSize,
                        stoppingToken);
                    instrumentation.RecordStoreOperation("claim",
                        Stopwatch.GetElapsedTime(claimStartedAt).TotalMilliseconds);

                    if (claimed.Count == 0)
                    {
                        await WaitForWakeupAsync(wakeup, stoppingToken);
                        continue;
                    }

                    var claimedAt = timeProvider.GetUtcNow();
                    foreach (var run in claimed)
                    {
                        instrumentation.RecordRunClaimed(run.JobName, run.NotBefore, claimedAt);

                        var executionTask = ExecuteClaimedRunAsync(run, stoppingToken);
                        _activeTasks[run.Id] = executionTask;
                        _ = executionTask.ContinueWith(completed =>
                            {
                                // Identity-keyed: a durable suspend->resume re-claims the same run id
                                // on a fresh execution task, which may already be registered under
                                // this key by the time this continuation runs. Compare-and-remove so
                                // we only evict our own entry and never strip the successor's
                                // capacity slot or shutdown-drain handle. Per-run cancellation and
                                // tracker state are removed by ExecuteClaimedRunAsync's finally
                                // (which runs before this continuation and is likewise identity-keyed).
                                ((ICollection<KeyValuePair<string, Task>>)_activeTasks)
                                    .Remove(new(run.Id, completed));
                                logEventPump.DropRunState(run.Id);
                                eventWriter.DropRunState(run.Id);
                            },
                            CancellationToken.None,
                            TaskContinuationOptions.None,
                            TaskScheduler.Default);
                    }

                    loopHealth.RecordSuccess(LoopName);
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

                    // Never crash the host on a single bad tick. Operators see the failure via
                    // metrics/health-check while the host stays up.
                    Log.ExecutorLoopFailed(logger, ex);
                    instrumentation.RecordLoopError(LoopName);
                    loopHealth.RecordFailure(LoopName);
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

        // Declared outside the try so the OCE catch can tell timeout from other cancellations,
        // and so failure paths can flush pending progress on `context` before transitioning.
        CancellationTokenSource? timeoutCts = null;
        CancellationTokenSource? executionCts = null;
        IDisposable? jobContextScope = null;
        TimeSpan? configuredTimeout = null;
        JobContext? context = null;
        var handlerCompleted = false;

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
                        "executor",
                        "no_handler_registered",
                        "No handler registered for this job.",
                        typeof(InvalidOperationException).FullName));

                if (options.CompiledOnDeadLetterCallbacks.Count > 0)
                {
                    await using var callbackScope = scopeFactory.CreateAsyncScope();
                    var callbackContext = CreateJobContext(
                        run,
                        deadLetterToken,
                        new InvalidOperationException("No handler registered for this job."));

                    await InvokeLifecycleCallbacksAsync(
                        options.CompiledOnDeadLetterCallbacks,
                        callbackContext,
                        callbackScope.ServiceProvider,
                        deadLetterToken);
                }

                var noHandlerTransition = RunStatusTransition.RunningToFailed(
                    run.Id,
                    run.LeaseEpoch,
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
                    null,
                    run,
                    noHandlerTransition,
                    deadLetterToken,
                    CancellationToken.None);
                if (dlResult.Transitioned)
                {
                    instrumentation.RecordRunFailed(run.JobName, run.StartedAt, timeProvider.GetUtcNow(),
                        DeadLetterReason.NoHandlerRegistered);
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

            // Timeout CTS is separate from runCts so the OCE catch can tell them apart.
            // TimeProvider-aware so FakeTimeProvider drives it in tests.
            configuredTimeout = job.Definition.Timeout;
            if (configuredTimeout is { } timeout)
            {
                timeoutCts = new(timeout, timeProvider);
                executionCts = CancellationTokenSource.CreateLinkedTokenSource(runCts.Token, timeoutCts.Token);
            }

            var executionToken = executionCts?.Token ?? runCts.Token;

            // Durable runs get a PendingAwaitSet that IJobClient methods populate before yielding;
            // the executor's catch-yield path reads it to assemble the wait set for the store's
            // TrySuspendRunAsync call. Non-durable runs don't allocate one.
            var pendingAwaits = job.Definition.IsDurable ? new PendingAwaitSet() : null;
            DurableExecutionSnapshot? snapshot = null;
            if (job.Definition.IsDurable)
            {
                // One bulk read at claim time replaces every per-call store round trip during the
                // handler's replay. IJobClient consults JobContext.DurableSnapshot before going to
                // the store for recorded child runs / batches, so replay cost stays O(handler CPU).
                snapshot = await InvokeStoreWithTransientRetryAsync(
                    ct => store.LoadExecutionSnapshotAsync(run.Id, ct),
                    "load-durable-snapshot",
                    executionToken);
            }

            context = CreateJobContext(run, executionToken, isDurable: job.Definition.IsDurable,
                pendingAwaits: pendingAwaits, durableSnapshot: snapshot);
            jobContextScope = JobContext.EnterScope(context);

            await using var scope = scopeFactory.CreateAsyncScope();

            var invocation = await ExecuteThroughFiltersAsync(job, run, context, scope.ServiceProvider, executionToken);
            context.ThrowIfRecordedStepsSkipped();
            handlerCompleted = true;
            context.Result = invocation.ResultObject;

            var completedAt = timeProvider.GetUtcNow();

            var completed = RunStatusTransition.RunningToSucceeded(
                run.Id,
                run.LeaseEpoch,
                completedAt,
                run.NotBefore,
                run.NodeName,
                1,
                invocation.ResultJson,
                null,
                run.StartedAt,
                completedAt);

            using var bestEffortCts = CreateBestEffortWorkCts(stoppingToken);

            var transitionResult = await FlushAndTransitionTerminalAsync(
                context, run, completed,
                bestEffortCts.Token,
                CancellationToken.None);
            if (transitionResult.Transitioned)
            {
                instrumentation.RecordRunCompleted(run.JobName, run.StartedAt, completedAt);
                await RunPostCompletionHousekeepingAsync(
                    job,
                    run,
                    context,
                    transitionResult,
                    scope.ServiceProvider,
                    bestEffortCts.Token);
            }
        }
        catch (Exception ex) when (UnwrapDurableYield(ex) is { })
        {
            // Handlers may surface DurableYieldException directly (canonical `await` form) or
            // wrapped in nested AggregateException (Task.WhenAll(...).Wait(), Parallel.For,
            // nested aggregating boundaries). Flatten-based unwrap catches every nesting depth.
            await SuspendOrResumeAsync(context, run, stoppingToken);
        }
        catch (Exception ex) when (context?.IsDurable == true && IsUncaughtChildFailure(ex))
        {
            // A recorded child terminated Failed and the orchestrator didn't catch the resulting
            // JobRunException — surfaced bare (a single awaited child) or wrapped in an
            // AggregateException (Task.WhenAll, batch waits). Replay re-derives the same child id,
            // reads the same recorded failure, and re-throws identically, so retrying only burns the
            // budget on a deterministic outcome. Dead-letter immediately. Canceled children are not
            // matched here; they fall through to the owned-cancellation path below so a canceled
            // child cancels the orchestrator rather than failing it, keeping the bare and aggregated
            // cancellation outcomes consistent.
            await HandleRunFailureAsync(context, run, ex, stoppingToken, true);
        }
        catch (DurableReplayMismatchException mismatch)
        {
            // The orchestrator code no longer matches its persisted replay history (steps skipped,
            // reordered, or a different operation/name recorded at a step). This is deterministic:
            // replaying the same handler against the same history reproduces the identical mismatch,
            // so retrying only burns the budget. Dead-letter immediately and tag it NonDeterministic
            // so operators can distinguish a code/history divergence (needs a code fix or rollback)
            // from genuine retry exhaustion.
            await HandleRunFailureAsync(context, run, mismatch, stoppingToken, true,
                DeadLetterReason.NonDeterministic);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (TryGetOwnedCancellationReason(ex, out var cancelReason))
            {
                await HandleRunCancellationAsync(context, run, ex, cancelReason, stoppingToken);
            }
            else
            {
                await HandleRunFailureAsync(context, run, ex, stoppingToken);
            }
        }
        catch (OperationCanceledException ex)
        {
            if (stoppingToken.IsCancellationRequested)
            {
                await HandleRunFailureAsync(
                    context,
                    run,
                    new ShutdownInterruptedException(ex),
                    stoppingToken);
                return;
            }

            // Timeout is a failure, not a cancellation: route through HandleRunFailureAsync so
            // it's retryable and emits an AttemptFailure event.
            if (timeoutCts is { IsCancellationRequested: true })
            {
                activity?.SetTag("surefire.job.timeout", true);
                await HandleRunFailureAsync(
                    context,
                    run,
                    new TimeoutException(
                        $"Job execution timed out after {configuredTimeout}.", ex),
                    stoppingToken);
                return;
            }

            // OCE after the handler completed came from post-handler bookkeeping (e.g. the
            // best-effort completion token), not a genuine cancel. Leave the run in Running so
            // stale recovery picks it up.
            if (!runCts.IsCancellationRequested && handlerCompleted)
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
                            "executor",
                            "bookkeeping_failure"),
                        stoppingToken);
                }
                catch (Exception appendEx)
                {
                    Log.PostCompletionHousekeepingFailed(logger, appendEx, run.Id, run.JobName);
                }

                return;
            }

            var cancelReason = runCts.IsCancellationRequested
                ? "Canceled by external request."
                : GetOperationCanceledMessage(ex);
            await HandleRunCancellationAsync(context, run, ex, cancelReason, stoppingToken);
        }
        finally
        {
            jobContextScope?.Dispose();
            executionCts?.Dispose();
            timeoutCts?.Dispose();
            // Identity-keyed: a durable suspend->resume re-claims the same run id on a fresh
            // execution task that may have already registered its own runCts under this key. Only
            // remove our own registration so we don't strip the successor's cancellation wiring.
            ((ICollection<KeyValuePair<string, CancellationTokenSource>>)_runCancellation)
                .Remove(new(run.Id, runCts));
            activeRunTracker.Remove(run.Id, runCts);
            try
            {
                runCts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }
    }

    private async Task HandleRunFailureAsync(JobContext? context, JobRun run, Exception ex,
        CancellationToken stoppingToken, bool forceDeadLetter = false,
        DeadLetterReason? deadLetterReason = null)
    {
        // Top-level catch so failure-handling failures don't escape as unobserved task exceptions
        // on the fire-and-forget executionTask continuation. We deliberately do NOT retry: the
        // store we'd write to is misbehaving, so let stale recovery reclaim the run the same way
        // it handles real node crashes. Catches OCE too (typically bestEffortToken expiring).
        try
        {
            await HandleRunFailureAsyncCore(context, run, ex, stoppingToken, forceDeadLetter,
                deadLetterReason);
        }
        catch (Exception handlingEx)
        {
            Log.FailureHandlingFailed(logger, handlingEx, run.Id, run.JobName);
        }
    }

    /// <summary>
    ///     Unwraps a thrown exception to a <see cref="DurableYieldException" /> if one is present
    ///     anywhere in its tree. Handles bare yields, single-level <see cref="AggregateException" />,
    ///     and nested aggregates (Task.WaitAll, Parallel.For, etc.) via <see cref="AggregateException.Flatten" />.
    /// </summary>
    private static DurableYieldException? UnwrapDurableYield(Exception ex) => ex switch
    {
        DurableYieldException yex => yex,
        AggregateException agg => agg.Flatten().InnerExceptions.OfType<DurableYieldException>().FirstOrDefault(),
        _ => null
    };

    private async Task SuspendOrResumeAsync(JobContext? context, JobRun run, CancellationToken stoppingToken)
    {
        using var bestEffortCts = CreateBestEffortWorkCts(stoppingToken);
        var bestEffortToken = bestEffortCts.Token;

        // Flush events before the suspend transition so subscribers don't see a non-Running
        // status with un-emitted child events.
        if (context is { })
        {
            await context.FlushPendingProgressAsync(bestEffortToken);
        }

        await logEventPump.FlushRunAsync(run.Id, bestEffortToken);
        await eventWriter.FlushRunAsync(run.Id, bestEffortToken);

        // Read the wait set the handler registered through its IJobClient calls. The set may
        // include both iterator-style yields (one id per throw) and cooperative-TCS awaits
        // (registered before the state machine yielded). Both write to the same PendingAwaitSet.
        var (awaitedRunIds, awaitedBatchIds) = context?.PendingAwaits?.Snapshot()
                                               ?? (Array.Empty<string>(), Array.Empty<string>());

        if (awaitedRunIds.Count == 0 && awaitedBatchIds.Count == 0)
        {
            // The handler yielded with no awaited entities. This indicates a determinism bug:
            // the handler awaited something other than an IJobClient call (Task.Delay, external
            // HTTP, etc.) or a code path that yields the executor's task without registering an
            // await. We can't replay productively from this state, so fail the run and surface
            // a clear message rather than tight-looping into endless suspends.
            await HandleRunFailureAsync(
                context,
                run,
                new InvalidOperationException(
                    "[surefire/durable/empty-await-set] " +
                    "Durable handler yielded with no awaited entities. Durable orchestrators must only " +
                    "await IJobClient methods (RunAsync, RunBatchAsync, WaitAsync, WaitBatchAsync, or " +
                    "iterator methods). Wrap external IO, timers, or non-deterministic work in a child " +
                    "job so its result is recorded for deterministic replay."),
                stoppingToken,
                true);
            return;
        }

        // The store decides atomically whether to park in Suspended (any awaited entity still
        // non-terminal) or transition straight back to Pending (all awaited entities already
        // terminal: handler should replay immediately rather than wait for a wake source that
        // will never arrive).
        var suspendResult = await InvokeStoreWithTransientRetryAsync(
            ct => store.TrySuspendRunAsync(run.Id, run.LeaseEpoch, awaitedRunIds, awaitedBatchIds,
                timeProvider.GetUtcNow(), ct),
            "suspend-durable",
            bestEffortToken);

        // Split metrics so a sustained ImmediatePending rate (handler yielded and re-claimed
        // without making progress) shows up as the operational fingerprint of a buggy handler.
        switch (suspendResult)
        {
            case DurableSuspendOutcome.Suspended:
                instrumentation.RecordDurableSuspended(run.JobName);
                break;
            case DurableSuspendOutcome.ImmediatePending:
                instrumentation.RecordDurableInstantResume(run.JobName);
                await notifications.PublishAsync(NotificationChannels.RunCreated, null, bestEffortToken);
                break;
            case DurableSuspendOutcome.NotTransitioned:
                await HandleDurableSuspendNotTransitionedAsync(context, run, bestEffortToken);
                break;
        }
    }

    private async Task HandleDurableSuspendNotTransitionedAsync(JobContext? context, JobRun run,
        CancellationToken cancellationToken)
    {
        var current = await InvokeStoreWithTransientRetryAsync(
            ct => store.GetRunAsync(run.Id, ct),
            "reload-durable-suspend-cas-miss",
            cancellationToken);

        if (current is null || current.Status != JobStatus.Running || current.LeaseEpoch != run.LeaseEpoch)
        {
            Log.DurableSuspendCasLost(logger, run.Id, run.JobName, run.LeaseEpoch, current?.Status,
                current?.LeaseEpoch);
            return;
        }

        await HandleRunFailureAsync(
            context,
            run,
            new InvalidOperationException(
                "Durable suspend CAS missed, but the run is still Running with the same lease epoch. " +
                "This indicates a store protocol violation."),
            cancellationToken,
            true);
    }

    private async Task HandleRunFailureAsyncCore(JobContext? context, JobRun run, Exception ex,
        CancellationToken stoppingToken, bool forceDeadLetter = false,
        DeadLetterReason? deadLetterReason = null)
    {
        // Activity.Current is the span StartActivity'd in ExecuteClaimedRunAsync. Mark it Error
        // so OTel backends don't show failed runs as Unset.
        var activity = Activity.Current;
        activity?.SetStatus(ActivityStatusCode.Error, ex.Message);
        activity?.AddException(ex);

        using var bestEffortCts = CreateBestEffortWorkCts(stoppingToken);
        var bestEffortToken = bestEffortCts.Token;
        var isShutdownInterruption = ex is ShutdownInterruptedException;

        IReadOnlyList<CompiledCallback> jobDeadLetterCallbacks = [];
        if (registry.TryGet(run.JobName, out var job))
        {
            jobDeadLetterCallbacks = job.OnDeadLetterCallbacks;
            var failureCount = isShutdownInterruption ? run.FailureCount : run.FailureCount + 1;

            var canRetry = !forceDeadLetter && failureCount <= job.Definition.RetryPolicy.MaxRetries;
            if (canRetry)
            {
                // Backoff is computed off real handler failures, not lease/replay bookkeeping.
                var notBefore = timeProvider.GetUtcNow() + backoff.NextDelay(failureCount - 1,
                    job.Definition.RetryPolicy.InitialDelay, job.Definition.RetryPolicy.MaxDelay,
                    job.Definition.RetryPolicy.Jitter, job.Definition.RetryPolicy.BackoffType);

                if (isShutdownInterruption)
                {
                    Log.AttemptInterruptedByShutdownRetrying(
                        logger,
                        failureCount,
                        notBefore,
                        job.Definition.RetryPolicy.MaxRetries);
                }
                else
                {
                    logger.LogWarning(
                        ex,
                        "Attempt {Attempt} failed. Retrying at {NotBefore} (max retries: {MaxRetries}).",
                        failureCount,
                        notBefore,
                        job.Definition.RetryPolicy.MaxRetries);
                }

                await using var retryScope = scopeFactory.CreateAsyncScope();
                // The retry callback sees the logical user-code attempt. The callback runs outside
                // the orchestrator scope, so IsReplaying is naturally false.
                var retryContext = CreateJobContext(run, bestEffortToken, ex,
                    job.Definition.IsDurable);

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
                        "executor",
                        GetFailureCode(ex)));

                var retryEvents = new List<RunEvent>();
                if (retryFailureEvent is { } rfe)
                {
                    retryEvents.Add(rfe);
                }

                // Retry path leaves Reason null; per-attempt failure detail lives on the
                // AttemptFailure event. Reason is reserved for non-exception termination causes.
                var pending = RunStatusTransition.RunningToPending(
                    run.Id,
                    run.LeaseEpoch,
                    notBefore,
                    null,
                    run.Result,
                    events: retryEvents.Count > 0 ? retryEvents : null);
                if (!isShutdownInterruption)
                {
                    pending.IncrementFailureCount = true;
                    pending.IncrementAttempt = true;
                }

                // Drain async-write paths so the retry attempt starts with no carry-over progress
                // from the failed attempt.
                if (context is { })
                {
                    await context.FlushPendingProgressAsync(bestEffortToken);
                }

                await logEventPump.FlushRunAsync(run.Id, bestEffortToken);
                await eventWriter.FlushRunAsync(run.Id, bestEffortToken);
                // Durable orchestrators retain their descendants across a handler-retry: the
                // recorded child runs are part of the orchestrator's replay history, and canceling
                // them would break determinism (replay would re-derive the same id, find it
                // Canceled, and propagate the cancel exception forever). Non-durable jobs own
                // their descendants per-attempt and start fresh on retry.
                if (!job.Definition.IsDurable)
                {
                    await CancelDescendantsWithTransientRetryAsync(
                        run.Id,
                        bestEffortToken,
                        $"Canceled because parent run '{run.Id}' attempt {run.Attempt} failed before retry.");
                }

                var pendingResult = await InvokeStoreWithTransientRetryAsync(
                    ct => store.TryTransitionRunAsync(pending, ct),
                    "transition-retry",
                    bestEffortToken);
                if (pendingResult.Transitioned)
                {
                    // Per-attempt failure event so retried-then-succeeded runs still show their
                    // failed attempts on the trace.
                    activity?.AddEvent(new("surefire.attempt.failed",
                        tags: new()
                        {
                            ["surefire.failure.reason"] = GetFailureCode(ex)
                        }));

                    await notifications.PublishAsync(NotificationChannels.RunCreated, null,
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
            // Dead-letter callback sees the logical user-code attempt.
            var callbackContext = CreateJobContext(run, bestEffortToken, ex,
                job is { Definition.IsDurable: true });

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
                "executor",
                GetFailureCode(ex)));

        // Dead-letter Reason is only set for non-exception causes. Shutdown interruption is an
        // environmental termination, so it surfaces on Reason for the dashboard to distinguish
        // from retry exhaustion. Retry exhaustion leaves Reason null; final-attempt detail is
        // on the AttemptFailure event.
        var deadLetter = RunStatusTransition.RunningToFailed(
            run.Id,
            run.LeaseEpoch,
            timeProvider.GetUtcNow(),
            run.NotBefore,
            run.NodeName,
            run.Progress,
            isShutdownInterruption ? "Shutdown interrupted mid-run." : null,
            run.Result,
            run.StartedAt,
            timeProvider.GetUtcNow(),
            failureEvent is { } ? [failureEvent] : null);
        deadLetter.IncrementFailureCount = !isShutdownInterruption;

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

        var transitionResult = await FlushAndTransitionTerminalAsync(
            context, run, deadLetter,
            bestEffortToken,
            CancellationToken.None);
        if (transitionResult.Transitioned)
        {
            instrumentation.RecordRunFailed(run.JobName, run.StartedAt, timeProvider.GetUtcNow(),
                deadLetterReason
                ?? (isShutdownInterruption
                    ? DeadLetterReason.ShutdownInterrupted
                    : DeadLetterReason.RetriesExhausted));
            await CancelDescendantsWithTransientRetryAsync(
                run.Id,
                bestEffortToken,
                $"Canceled because parent run '{run.Id}' failed.");

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

    private async Task HandleRunCancellationAsync(JobContext? context, JobRun run, Exception ex, string reason,
        CancellationToken stoppingToken)
    {
        var activity = Activity.Current;
        activity?.SetStatus(ActivityStatusCode.Error, "Canceled");
        activity?.AddException(ex);

        using var bestEffortCts = CreateBestEffortWorkCts(stoppingToken);
        var bestEffortToken = bestEffortCts.Token;
        var canceledAt = timeProvider.GetUtcNow();

        var cancelEvent = batchCompletionHandler.CreateFailureEvent(
            run,
            RunFailureEnvelope.FromException(
                run.Attempt,
                canceledAt,
                ex,
                "executor",
                "run_canceled"));

        if (context is { })
        {
            await context.FlushPendingProgressAsync(bestEffortToken);
        }

        await logEventPump.FlushRunAsync(run.Id, bestEffortToken);
        await eventWriter.FlushRunAsync(run.Id, bestEffortToken);
        var cancelResult = await InvokeStoreWithTransientRetryAsync(
            ct => store.TryCancelRunAsync(
                run.Id,
                run.LeaseEpoch,
                reason,
                cancelEvent is { } ? [cancelEvent] : null,
                ct),
            "cancel",
            bestEffortToken);

        await CancelDescendantsWithTransientRetryAsync(run.Id, bestEffortToken);

        if (cancelResult.Transitioned)
        {
            instrumentation.RecordRunCanceled(run.JobName, run.StartedAt, canceledAt);
            await notifications.PublishAsync(NotificationChannels.RunTerminated(run.Id), run.Id,
                bestEffortToken);
            await notifications.PublishAsync(NotificationChannels.RunAvailable, run.Id, bestEffortToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(run.Id), run.Id, bestEffortToken);
            if (run.BatchId is { } canceledBatchId)
            {
                await notifications.PublishAsync(NotificationChannels.RunEvent(canceledBatchId), run.Id,
                    bestEffortToken);
            }

            if (cancelResult.BatchCompletion is { } bc)
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

    private JobContext CreateJobContext(JobRun run, CancellationToken cancellationToken,
        Exception? exception = null, bool isDurable = false, PendingAwaitSet? pendingAwaits = null,
        DurableExecutionSnapshot? durableSnapshot = null)
    {
        var attempt = run.Attempt;
        string? orchestratorRunId = null;
        var highestRecordedStep = 0;
        if (isDurable)
        {
            orchestratorRunId = run.Id;
            highestRecordedStep = run.HighestRecordedStep;
        }

        return new()
        {
            RunId = run.Id,
            RootRunId = run.RootRunId ?? run.Id,
            JobName = run.JobName,
            Attempt = attempt,
            BatchId = run.BatchId,
            CancellationToken = cancellationToken,
            Store = store,
            Notifications = notifications,
            EventWriter = eventWriter,
            TimeProvider = timeProvider,
            SerializerOptions = options.SerializerOptions,
            NodeName = run.NodeName ?? options.NodeName,
            Exception = exception,
            OrchestratorRunId = orchestratorRunId,
            HighestRecordedStep = highestRecordedStep,
            PendingAwaits = pendingAwaits,
            DurableSnapshot = durableSnapshot
        };
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

    private async Task<RunTransitionResult> FlushAndTransitionTerminalAsync(JobContext? context, JobRun run,
        RunStatusTransition transition, CancellationToken flushToken, CancellationToken transitionToken)
    {
        // Flush pending progress, logs, and events before the terminal transition so observers
        // never see the final status before earlier events are durable.
        if (context is { })
        {
            await context.FlushPendingProgressAsync(flushToken);
        }

        await logEventPump.FlushRunAsync(run.Id, flushToken);
        await eventWriter.FlushRunAsync(run.Id, flushToken);

        return await InvokeStoreWithTransientRetryAsync(
            ct => store.TryTransitionRunAsync(transition, ct),
            "transition",
            transitionToken);
    }

    private async Task CancelDescendantsWithTransientRetryAsync(string runId, CancellationToken cancellationToken,
        string? fixedReason = null)
    {
        await InvokeStoreWithTransientRetryAsync(
            async ct =>
            {
                await runCancellation.CancelDescendantsAsync(runId, ct, fixedReason);
                return true;
            },
            "cancel-descendants",
            cancellationToken);
    }

    private async Task<T> InvokeStoreWithTransientRetryAsync<T>(
        Func<CancellationToken, Task<T>> operation,
        string operationName,
        CancellationToken cancellationToken)
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                return await operation(cancellationToken);
            }
            catch (Exception ex) when (store.IsTransientException(ex))
            {
                Log.StoreCallTransientRetrying(logger, ex, operationName, attempt);
                instrumentation.RecordStoreRetry(operationName);
                await Task.Delay(backoff.NextDelay(attempt++, TransientRetryInitial, TransientRetryMax),
                    timeProvider, cancellationToken);
            }
        }
    }


    private async Task<InvocationResult> ExecuteThroughFiltersAsync(RegisteredJob registration, JobRun run,
        JobContext context, IServiceProvider services, CancellationToken cancellationToken)
    {
        var allFilterFactories = new List<Func<IServiceProvider, IJobFilter>>(
            options.FilterFactories.Count + registration.FilterFactories.Count);
        allFilterFactories.AddRange(options.FilterFactories);
        allFilterFactories.AddRange(registration.FilterFactories);

        var invocation = InvocationResult.Empty;
        JobFilterDelegate pipeline = async _ =>
        {
            invocation = await InvokeHandlerAsync(registration, run, context, services, cancellationToken);
        };

        for (var i = allFilterFactories.Count - 1; i >= 0; i--)
        {
            var filter = allFilterFactories[i](services);
            var next = pipeline;
            pipeline = ctx => filter.InvokeAsync(ctx, next);
        }

        await pipeline(context);
        return invocation;
    }

    private async Task<InvocationResult> InvokeHandlerAsync(RegisteredJob registration, JobRun run, JobContext context,
        IServiceProvider services, CancellationToken cancellationToken)
    {
        var descriptor = registration.Descriptor;
        var args = await BindParametersAsync(descriptor, run, context, services, cancellationToken);

        var invocationResult = descriptor.Invoke(args, descriptor.Handler);

        var (hasResult, value) = await AwaitResultAsync(invocationResult, descriptor);
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

        if (descriptor.AsyncEnumerableElementType is { })
        {
            var materializer = descriptor.Materializer
                               ?? throw new InvalidOperationException(
                                   $"Job '{run.JobName}' returned IAsyncEnumerable but its descriptor carries no " +
                                   "Materializer. The descriptor is malformed; re-register via app.AddJob(\"...\", handler) " +
                                   "or supply Materializer on a hand-built JobRegistrationDescriptor.");
            var elementTypeInfo = descriptor.OutputStreamElementJsonTypeInfoFactory?.Invoke(options.SerializerOptions)
                                  ?? throw new InvalidOperationException(
                                      $"Job '{run.JobName}' returned IAsyncEnumerable but its descriptor carries no " +
                                      "OutputStreamElementJsonTypeInfoFactory. The descriptor is malformed; re-register via " +
                                      "app.AddJob(\"...\", handler) or supply OutputStreamElementJsonTypeInfoFactory on a " +
                                      "hand-built JobRegistrationDescriptor.");
            var items = await materializer(this, value, elementTypeInfo, run, cancellationToken);
            var resultJson = $"[{string.Join(',', items)}]";
            return new(
                resultJson,
                value,
                items);
        }

        var resultTypeInfo = descriptor.ResultJsonTypeInfoFactory?.Invoke(options.SerializerOptions)
                             ?? throw new InvalidOperationException(
                                 $"Job '{run.JobName}' produced a result but its descriptor carries no " +
                                 "ResultJsonTypeInfoFactory. The job was registered through a path that " +
                                 "skipped result-type metadata; re-register via app.AddJob(\"...\", handler) " +
                                 "or supply ResultJsonTypeInfoFactory on a hand-built JobRegistrationDescriptor.");
        return new(
            JsonSerializer.Serialize(value, resultTypeInfo),
            value,
            null);
    }

    private async Task<object?[]> BindParametersAsync(JobRegistrationDescriptor descriptor, JobRun run,
        JobContext context, IServiceProvider services, CancellationToken cancellationToken)
    {
        var parameters = descriptor.Parameters;
        using var doc = run.Arguments is null ? null : JsonDocument.Parse(run.Arguments);
        var root = doc?.RootElement;
        var declaredInputArguments = await GetDeclaredInputArgumentsAsync(run.Id, cancellationToken);
        var values = new object?[parameters.Count];
        var streamBinders = descriptor.StreamBinders;
        var inputJsonBinders = descriptor.InputJsonBinders;
        var typeInfoFactories = descriptor.ParameterJsonTypeInfoFactories;
        var streamTypeInfoFactories = descriptor.StreamParameterJsonTypeInfoFactories;

        for (var i = 0; i < parameters.Count; i++)
        {
            var parameter = parameters[i];

            switch (parameter.Kind)
            {
                case ParameterKind.JobContext:
                    values[i] = context;
                    continue;
                case ParameterKind.CancellationToken:
                    values[i] = context.CancellationToken;
                    continue;
                case ParameterKind.ServiceProvider:
                    values[i] = services;
                    continue;
                case ParameterKind.Service:
                    values[i] = services.GetRequiredService(parameter.Type);
                    continue;
            }

            if (parameter.Kind == ParameterKind.ServiceOrJson)
            {
                var service = services.GetService(parameter.Type);
                if (service is { })
                {
                    values[i] = service;
                    continue;
                }
            }

            if (parameter.Kind == ParameterKind.Stream
                && streamBinders?[i] is { } binder
                && TryGetDeclaredInputArgumentName(parameters, parameter, root, declaredInputArguments,
                    out var argumentName))
            {
                var streamTypeInfo = streamTypeInfoFactories?[i]?.Invoke(options.SerializerOptions)
                                     ?? throw new InvalidOperationException(
                                         $"Parameter '{parameter.Name}' has no StreamParameterJsonTypeInfoFactory. The job " +
                                         "was registered through a path that skipped stream-parameter metadata; re-register " +
                                         "via app.AddJob(\"...\", handler) or supply StreamParameterJsonTypeInfoFactories on " +
                                         "a hand-built JobRegistrationDescriptor.");
                values[i] = await binder(this, run, argumentName, streamTypeInfo, cancellationToken);
                continue;
            }

            JsonElement? jsonValue = null;
            if (root is { ValueKind: JsonValueKind.Object }
                && TryGetJsonProperty(root.Value, parameter.Name, out var property))
            {
                jsonValue = property;
            }
            else if (root.HasValue && root.Value.ValueKind != JsonValueKind.Object)
            {
                jsonValue = root.Value;
            }

            if (jsonValue is { } valueElement)
            {
                if (parameter.Kind == ParameterKind.Stream)
                {
                    if (inputJsonBinders?[i] is not { } jsonBinder)
                    {
                        throw new InvalidOperationException(
                            $"Stream parameter '{parameter.Name}' on job '{context.JobName}' has no " +
                            "declared input stream and no inline JSON binder. Hand-built descriptors must " +
                            "populate InputJsonBinders[i] for ParameterKind.Stream parameters that can be " +
                            "bound from inline JSON arguments.");
                    }

                    values[i] = jsonBinder(valueElement, options.SerializerOptions);
                }
                else
                {
                    values[i] = BindJsonValue(valueElement, parameter,
                        typeInfoFactories?[i], options.SerializerOptions);
                }

                continue;
            }

            if (parameter.HasDefault)
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
        IReadOnlyList<ParameterDescriptor> parameters,
        ParameterDescriptor parameter,
        JsonElement? root,
        HashSet<string> declaredInputArguments,
        out string argumentName)
    {
        argumentName = string.Empty;
        if (string.IsNullOrEmpty(parameter.Name))
        {
            return false;
        }

        if (declaredInputArguments.Contains(parameter.Name))
        {
            argumentName = parameter.Name;
            return true;
        }

        if (!root.HasValue
            && parameters.Count == 1
            && declaredInputArguments.Contains("$root"))
        {
            argumentName = "$root";
            return true;
        }

        return false;
    }

    private async IAsyncEnumerable<T> ReadInputStreamCoreAsync<T>(string runId, string argumentName,
        Func<string, T> deserialize,
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
                        yield return deserialize(envelope.Payload);
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

    private static object? BindJsonValue(
        JsonElement value,
        ParameterDescriptor parameter,
        JsonTypeInfoFactory? typeInfoFactory,
        JsonSerializerOptions serializerOptions)
    {
        var typeInfo = typeInfoFactory?.Invoke(serializerOptions)
                       ?? throw new InvalidOperationException(
                           $"Parameter '{parameter.Name}' has no ParameterJsonTypeInfoFactory. The job " +
                           "was registered through a path that skipped parameter-type metadata; " +
                           "re-register via app.AddJob(\"...\", handler) or supply " +
                           "ParameterJsonTypeInfoFactories on a hand-built JobRegistrationDescriptor.");

        return JsonSerializer.Deserialize(value.GetRawText(), typeInfo);
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
        JobRegistrationDescriptor descriptor)
    {
        var hasResult = descriptor.ReturnKind is not (ReturnKind.Void or ReturnKind.Task or ReturnKind.ValueTask);
        if (!hasResult)
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
                return (true, descriptor.ExtractTaskResult!(task));
            case ValueTask:
                return (false, null);
            default:
            {
                if (descriptor.AsTask is { } asTask)
                {
                    var task = asTask(invocationResult);
                    await task;
                    return (true, descriptor.ExtractTaskResult!(task));
                }

                return (true, invocationResult);
            }
        }
    }

    private async Task<IReadOnlyList<string>> WriteOutputStreamCoreAsync<T>(
        IAsyncEnumerable<T> stream,
        Func<T, string> serialize,
        JobRun run,
        CancellationToken cancellationToken)
    {
        var items = new List<string>();
        await foreach (var item in stream.WithCancellation(cancellationToken))
        {
            var payload = serialize(item);
            items.Add(payload);
            await AppendOutputEventAsync(run, payload, cancellationToken);
        }

        await AppendOutputCompleteEventAsync(run, cancellationToken);

        return items;
    }

    private async Task AppendOutputEventAsync(JobRun run, string payload,
        CancellationToken cancellationToken)
    {
        await eventWriter.EnqueueAsync(
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.Output,
                Payload = payload,
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = run.Attempt
            },
            BuildRunEventNotifications(run),
            cancellationToken);
    }

    private async Task AppendOutputCompleteEventAsync(JobRun run, CancellationToken cancellationToken)
    {
        await eventWriter.EnqueueAsync(
            new()
            {
                RunId = run.Id,
                EventType = RunEventType.OutputComplete,
                Payload = "{}",
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = run.Attempt
            },
            BuildRunEventNotifications(run),
            cancellationToken);
    }

    private static BatchedEventWriter.NotificationPublish[] BuildRunEventNotifications(JobRun run) =>
        run.BatchId is { } batchId
            ? [new(NotificationChannels.RunEvent(run.Id), run.Id), new(NotificationChannels.RunEvent(batchId), run.Id)]
            : [new(NotificationChannels.RunEvent(run.Id), run.Id)];

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
            ExpiresAt = JobRunDefaults.GetDefaultExpiresAt(now, options),
            Priority = job.Definition.Priority,
            Progress = 0,
            DeduplicationId = BuildContinuousDeduplicationId(completedRun.Id),
            Attempt = 1
        };

        try
        {
            if (!await store.TryCreateRunAsync(
                    nextRun,
                    job.Definition.MaxConcurrency ?? 1,
                    cancellationToken: cancellationToken))
            {
                return;
            }
        }
        catch (RunConflictException)
        {
            return;
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);
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
        try
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
        finally
        {
            // Flush any pending progress synchronously so a queued timer can't fire after the
            // callback scope disposes.
            await context.FlushPendingProgressAsync(cancellationToken);
        }
    }

    // Snake_case per OTel tag-value convention. Emitted on the surefire.failure.reason activity
    // tag and (via AttemptFailure payload) the dashboard's per-attempt failure rendering.
    private static string GetFailureCode(Exception exception) => exception switch
    {
        ShutdownInterruptedException => "shutdown_interrupted",
        DurableReplayMismatchException => "durable_replay_mismatch",
        OperationCanceledException => "operation_canceled",
        _ => "exception"
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

    private static string GetOperationCanceledMessage(OperationCanceledException ex) =>
        string.IsNullOrWhiteSpace(ex.Message) ? "The operation was canceled." : ex.Message;

    /// <summary>
    ///     True when <paramref name="ex" /> is an uncaught durable child failure: a
    ///     <see cref="JobRunException" /> with <see cref="JobStatus.Failed" />, either bare (a single
    ///     awaited child) or wrapped in an <see cref="AggregateException" /> (Task.WhenAll, batch
    ///     waits). Uses the same all-match semantics as <see cref="TryGetOwnedCancellationReason" />:
    ///     an aggregate qualifies only when <em>every</em> flattened inner is a Failed child, so an
    ///     aggregate mixing a child failure with a transient or non-child exception falls through to
    ///     the normal retryable failure path instead of being dead-lettered.
    /// </summary>
    private static bool IsUncaughtChildFailure(Exception ex)
    {
        if (ex is JobRunException { Status: JobStatus.Failed })
        {
            return true;
        }

        if (ex is AggregateException aggregate)
        {
            var flattened = aggregate.Flatten().InnerExceptions;
            if (flattened.Count == 0)
            {
                return false;
            }

            foreach (var inner in flattened)
            {
                if (inner is not JobRunException { Status: JobStatus.Failed })
                {
                    return false;
                }
            }

            return true;
        }

        return false;
    }

    private static bool TryGetOwnedCancellationReason(Exception ex, out string reason)
    {
        if (ex is JobRunException { Status: JobStatus.Canceled } childCanceled)
        {
            reason = childCanceled.Reason is { Length: > 0 }
                ? $"Canceled because owned child run '{childCanceled.RunId}' was canceled: {childCanceled.Reason}"
                : $"Canceled because owned child run '{childCanceled.RunId}' was canceled.";
            return true;
        }

        if (ex is AggregateException aggregate)
        {
            var flattened = aggregate.Flatten().InnerExceptions;
            if (flattened.Count == 0)
            {
                reason = string.Empty;
                return false;
            }

            foreach (var inner in flattened)
            {
                if (inner is not JobRunException { Status: JobStatus.Canceled })
                {
                    reason = string.Empty;
                    return false;
                }
            }

            reason = flattened.Count == 1
                ? "Canceled because an owned child run was canceled."
                : "Canceled because owned child runs were canceled.";
            return true;
        }

        reason = string.Empty;
        return false;
    }

    private async Task WaitForActiveRunsAsync()
    {
        if (_activeTasks.Count == 0)
        {
            return;
        }

        // Cancel active runs and wait up to ShutdownTimeout for cleanup (state transitions,
        // failure events, notifications).
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

        [LoggerMessage(EventId = 1110, Level = LogLevel.Debug,
            Message = "Store call '{Operation}' hit transient error; retrying (attempt {Attempt}).")]
        public static partial void StoreCallTransientRetrying(ILogger logger, Exception exception, string operation,
            int attempt);

        [LoggerMessage(EventId = 1111, Level = LogLevel.Error,
            Message =
                "Failed to drive run '{RunId}' (job '{JobName}') to a terminal state from the failure handler. " +
                "Run will appear orphaned in Running until stale recovery picks it up after InactiveThreshold.")]
        public static partial void FailureHandlingFailed(ILogger logger, Exception exception, string runId,
            string jobName);

        [LoggerMessage(EventId = 1112, Level = LogLevel.Information,
            Message =
                "Durable suspend CAS for run '{RunId}' (job '{JobName}', lease epoch {LeaseEpoch}) lost to a concurrent transition. Current status: {CurrentStatus}; current lease epoch: {CurrentLeaseEpoch}.")]
        public static partial void DurableSuspendCasLost(ILogger logger, string runId, string jobName, long leaseEpoch,
            JobStatus? currentStatus, long? currentLeaseEpoch);
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
