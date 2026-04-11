using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Surefire;

/// <summary>
///     Default implementation of <see cref="IJobClient" /> built on top of the store and notification contracts.
/// </summary>
internal sealed partial class JobClient(
    IJobStore store,
    INotificationProvider notifications,
    TimeProvider timeProvider,
    SurefireOptions options,
    ILogger<JobClient> logger) : IJobClient
{
    private readonly JsonSerializerOptions _serializerOptions = options.SerializerOptions;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _inputPumpTokens = new(StringComparer.Ordinal);

    // Primitives
    public async Task<JobRun> TriggerAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default)
    {
        var runOptions = options ?? new();
        var prepared = PrepareArguments(args);
        var requestedPriority = await ResolveRequestedPriorityAsync(job, runOptions.Priority, cancellationToken);
        var run = CreateRun(job, prepared.SerializedArguments, runOptions, timeProvider.GetUtcNow(), requestedPriority ?? 0);
        var initialEvents = BuildInitialEvents(run.Id, prepared.StreamDeclaration);
        var created = await store.TryCreateRunAsync(run, initialEvents: initialEvents, cancellationToken: cancellationToken);
        if (!created)
            throw new RunConflictException(run.Id, $"Run creation for job '{job}' was rejected.");
        await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);
        if (prepared.Streams.Count > 0)
            StartInputPump(run.Id, prepared.Streams);
        return AttachSerializerOptions(run);
    }

    public async Task<JobBatch> TriggerBatchAsync(IEnumerable<BatchItem> runs, RunOptions? options = null, CancellationToken cancellationToken = default)
    {
        // Optionally apply options to each BatchItem if provided
        var items = options == null ? runs : runs.Select(b => b with { Options = options });
        return await TriggerBatchCoreAsync(items, cancellationToken);
    }

    public async Task<JobBatch> TriggerBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default)
    {
        var items = args.Select(a => new BatchItem(job, a, options));
        return await TriggerBatchCoreAsync(items, cancellationToken);
    }

    // Sugar
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public async Task<T> RunAsync<T>(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default)
    {
        var run = await TriggerAsync(job, args, options, cancellationToken);
        try
        {
            return await WaitAsync<T>(run.Id, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryCancelOwnedRunAsync(run.Id);
            throw;
        }
    }

    public async Task RunAsync(string job, object? args = null, RunOptions? options = null, CancellationToken cancellationToken = default)
    {
        var run = await TriggerAsync(job, args, options, cancellationToken);
        try
        {
            await WaitAsync(run.Id, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryCancelOwnedRunAsync(run.Id);
            throw;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public async IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var run = await TriggerAsync(job, args, options, cancellationToken);
        await foreach (var item in OwnedRunStreamAsync<T>(run.Id, cancellationToken))
            yield return item;
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public async Task<T[]> RunBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default)
        => await RunAllAsync<T>(job, args, options ?? new(), cancellationToken);

    public async Task RunBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null, CancellationToken cancellationToken = default)
        => await RunAllAsync(job, args, options ?? new(), cancellationToken);

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public async IAsyncEnumerable<T> StreamBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(job, args, options, cancellationToken);
        await foreach (var item in StreamEachAsync<T>(batch.Id, cancellationToken))
            yield return item.Item;
    }

    // Query
    public async Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default)
    {
        var record = await store.GetRunAsync(runId, cancellationToken);
        return record is null ? null : AttachSerializerOptions(record);
    }

    public async IAsyncEnumerable<JobRun> GetRunsAsync(RunFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var skip = 0;
        const int pageSize = 200;

        while (true)
        {
            var page = await store.GetRunsAsync(filter, skip, pageSize, cancellationToken);
            if (page.Items.Count == 0)
            {
                yield break;
            }

            foreach (var run in page.Items)
            {
                yield return AttachSerializerOptions(run);
            }

            skip += page.Items.Count;
        }
    }

    public async Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var batch = await store.GetBatchAsync(batchId, cancellationToken);
        return batch;
    }

    // Control
    public async Task CancelAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        await CancelRunAndDescendantsAsync(runId, new HashSet<string>(StringComparer.Ordinal), cancellationToken);
    }

    public async Task CancelBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        await store.CancelBatchRunsAsync(batchId, cancellationToken);

        // Pending runs that were cancelled never go through the executor, so the batch won't
        // be completed via MaybeCompleteBatchAsync. Complete it here as Cancelled.
        var completedAt = timeProvider.GetUtcNow();
        if (await store.TryCompleteBatchAsync(batchId, JobStatus.Cancelled, completedAt, cancellationToken))
        {
            await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId, cancellationToken);
        }
    }

    public async Task<JobRun> RerunAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        // Batch rerun: not supported via RerunAsync — create a new batch using TriggerBatchAsync instead.
        var existingBatch = await store.GetBatchAsync(runId, cancellationToken);
        if (existingBatch is { })
        {
            throw new InvalidOperationException(
                $"'{runId}' is a batch ID. To rerun a batch, retrieve the runs with GetRunsAsync and call TriggerBatchAsync.");
        }

        // Single-run rerun.
        var requestedPriority = await ResolveRequestedPriorityAsync(run.JobName, null, cancellationToken);
        var rerun = CreateRun(
            run.JobName,
            run.Arguments,
            new(),
            timeProvider.GetUtcNow(),
            requestedPriority ?? 0,
            rerunOfRunId: run.Id);

        var clonedInputEvents = await BuildClonedRunScopedInputEventsAsync(runId, rerun.Id, cancellationToken);

        var created = await store.TryCreateRunAsync(
            rerun,
            initialEvents: clonedInputEvents,
            cancellationToken: cancellationToken);
        if (!created)
        {
            throw new RunConflictException(runId, $"Run creation for rerun of '{runId}' was rejected.");
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, rerun.Id, cancellationToken);
        return AttachSerializerOptions(rerun);
    }

    public IAsyncEnumerable<RunObservation> ObserveAsync(string runId,
        CancellationToken cancellationToken = default) =>
        ObserveAsync(runId, RunEventCursor.Start, cancellationToken);

    public async IAsyncEnumerable<RunObservation> ObserveAsync(string runId, RunEventCursor cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var eventSub = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(runId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);
        await using var completionSub = await notifications.SubscribeAsync(
            NotificationChannels.RunTerminated(runId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        var sinceId = cursor.SinceEventId;
        while (true)
        {
            var run = await store.GetRunAsync(runId, cancellationToken);
            if (run is null)
            {
                throw new RunNotFoundException(runId);
            }

            var events = await store.GetEventsAsync(runId, sinceId, cancellationToken: cancellationToken);
            foreach (var @event in events)
            {
                sinceId = @event.Id;
                yield return new()
                {
                    Run = AttachSerializerOptions(run),
                    Event = @event,
                    Cursor = new() { SinceEventId = sinceId }
                };
            }

            if (run.Status.IsTerminal)
            {
                var terminalDrainEmitted = false;
                while (true)
                {
                    var terminalEvents = await store.GetEventsAsync(runId, sinceId, cancellationToken: cancellationToken);
                    if (terminalEvents.Count == 0)
                    {
                        break;
                    }

                    terminalDrainEmitted = true;
                    foreach (var @event in terminalEvents)
                    {
                        sinceId = @event.Id;
                        yield return new()
                        {
                            Run = AttachSerializerOptions(run),
                            Event = @event,
                            Cursor = new() { SinceEventId = sinceId }
                        };
                    }
                }

                if (!terminalDrainEmitted)
                {
                    var graceDeadline = timeProvider.GetUtcNow() + GetTerminalEventGracePeriod();
                    while (timeProvider.GetUtcNow() < graceDeadline)
                    {
                        var remaining = graceDeadline - timeProvider.GetUtcNow();
                        if (remaining <= TimeSpan.Zero)
                        {
                            break;
                        }

                        await WaitForWakeupAsync(wakeup, remaining, cancellationToken);

                        while (true)
                        {
                            var graceEvents = await store.GetEventsAsync(runId, sinceId, cancellationToken: cancellationToken);
                            if (graceEvents.Count == 0)
                            {
                                break;
                            }

                            foreach (var @event in graceEvents)
                            {
                                sinceId = @event.Id;
                                yield return new()
                                {
                                    Run = AttachSerializerOptions(run),
                                    Event = @event,
                                    Cursor = new() { SinceEventId = sinceId }
                                };
                            }
                        }
                    }
                }

                yield return new()
                {
                    Run = AttachSerializerOptions(run),
                    Event = null,
                    Cursor = new() { SinceEventId = sinceId }
                };
                yield break;
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
    }

    internal Task<string> TriggerAllAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default) =>
        TriggerAllAsync(jobName, argsList, new(), cancellationToken);

    internal async Task<string> TriggerAllAsync(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        if (options.DeduplicationId is { })
        {
            throw new ArgumentException(
                "DeduplicationId is not supported with batch operations. " +
                "Use TriggerAsync for deduplication-controlled runs.",
                nameof(options));
        }

        var requestedPriority = await ResolveRequestedPriorityAsync(jobName, options.Priority, cancellationToken);

        var args = argsList;
        if (!args.TryGetNonEnumeratedCount(out var argsCount))
        {
            var materializedArgs = argsList.ToArray();
            args = materializedArgs;
            argsCount = materializedArgs.Length;
        }

        var now = timeProvider.GetUtcNow();
        var batchId = CreateRunId();
        var batch = new JobBatch { Id = batchId, Status = JobStatus.Pending, Total = argsCount, CreatedAt = now };

        var runs = new List<JobRun>(argsCount);
        var streamPumps = new List<(string RunId, IReadOnlyList<StreamingArgumentSource> Streams,
            InputDeclarationEnvelope Declaration)>();
        foreach (var item in args)
        {
            var prepared = PrepareArguments(item);
            var child = CreateRun(jobName, prepared.SerializedArguments, options, now, requestedPriority ?? 0);
            child = child with { BatchId = batchId, RootRunId = child.RootRunId ?? batchId };
            runs.Add(child);

            if (prepared.Streams.Count > 0)
            {
                streamPumps.Add((child.Id, prepared.Streams, prepared.StreamDeclaration!));
            }
        }

        var initialEvents = new List<RunEvent>(streamPumps.Count);
        foreach (var (runId, _, declaration) in streamPumps)
        {
            initialEvents.AddRange(BuildInitialEvents(runId, declaration));
        }

        await store.CreateBatchAsync(batch, runs, initialEvents, cancellationToken);

        if (argsCount == 0)
        {
            var completedAt = timeProvider.GetUtcNow();
            if (await store.TryCompleteBatchAsync(batchId, JobStatus.Succeeded, completedAt, cancellationToken))
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId, cancellationToken);
            return batchId;
        }

        foreach (var (runId, streams, _) in streamPumps)
        {
            StartInputPump(runId, streams);
        }

        return batchId;
    }

    /// <inheritdoc />
    public async Task<JobBatch> TriggerBatchCoreAsync(IEnumerable<BatchItem> items,
        CancellationToken cancellationToken = default)
    {
        var itemsList = items.ToList();
        var now = timeProvider.GetUtcNow();
        var batchId = CreateRunId();
        var batch = new JobBatch { Id = batchId, Status = JobStatus.Pending, Total = itemsList.Count, CreatedAt = now };

        var runs = new List<JobRun>(itemsList.Count);
        var streamPumps = new List<(string RunId, IReadOnlyList<StreamingArgumentSource> Streams,
            InputDeclarationEnvelope Declaration)>();
        foreach (var item in itemsList)
        {
            var requestedPriority = await ResolveRequestedPriorityAsync(item.JobName, item.Options?.Priority,
                cancellationToken);
            var prepared = PrepareArguments(item.Args);
            var child = CreateRun(item.JobName, prepared.SerializedArguments, item.Options ?? new(), now,
                requestedPriority ?? 0);
            child = child with { BatchId = batchId, RootRunId = child.RootRunId ?? batchId };
            runs.Add(child);

            if (prepared.Streams.Count > 0)
            {
                streamPumps.Add((child.Id, prepared.Streams, prepared.StreamDeclaration!));
            }
        }

        var initialEvents = new List<RunEvent>(streamPumps.Count);
        foreach (var (runId, _, declaration) in streamPumps)
        {
            initialEvents.AddRange(BuildInitialEvents(runId, declaration));
        }

        await store.CreateBatchAsync(batch, runs, initialEvents, cancellationToken);

        if (itemsList.Count == 0)
        {
            var completedAt = timeProvider.GetUtcNow();
            if (await store.TryCompleteBatchAsync(batchId, JobStatus.Succeeded, completedAt, cancellationToken))
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId, cancellationToken);
        }
        else
        {
            foreach (var (runId, streams, _) in streamPumps)
            {
                StartInputPump(runId, streams);
            }
        }

        return batch;
    }

    /// <inheritdoc />
    public async Task<JobBatch> RunBatchAsync(IEnumerable<BatchItem> items,
        CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchCoreAsync(items, cancellationToken);
        await foreach (var _ in WaitEachAsync(batch.Id, cancellationToken)) { }
        var updated = await store.GetBatchAsync(batch.Id, cancellationToken);
        return updated ?? batch;
    }

    private async Task<RunResult[]> RunAllAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default)
    {
        var results = new List<RunResult>();
        await foreach (var result in RunEachAsync(jobName, argsList, cancellationToken))
        {
            results.Add(result);
        }

        return results.ToArray();
    }

    private async Task<RunResult[]> RunAllAsync(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        var results = new List<RunResult>();
        await foreach (var result in RunEachAsync(jobName, argsList, options, cancellationToken))
        {
            results.Add(result);
        }

        return results.ToArray();
    }

    private async Task<T[]> RunAllAsync<T>(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default)
    {
        var results = await RunAllAsync(jobName, argsList, cancellationToken);
        return ConvertBatchResults<T>(results);
    }

    private async Task<T[]> RunAllAsync<T>(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        var results = await RunAllAsync(jobName, argsList, options, cancellationToken);
        return ConvertBatchResults<T>(results);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<RunResult> RunEachAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default)
        => RunEachCoreAsync(
            triggerBatch: ct => TriggerAllAsync(jobName, argsList, ct),
            cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<RunResult> RunEachAsync(string jobName, IEnumerable<object?> argsList,
        RunOptions options, CancellationToken cancellationToken = default)
        => RunEachCoreAsync(
            triggerBatch: ct => TriggerAllAsync(jobName, argsList, options, ct),
            cancellationToken);

    private async IAsyncEnumerable<RunResult> RunEachCoreAsync(
        Func<CancellationToken, Task<string>> triggerBatch,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var batchId = await triggerBatch(cancellationToken);
        await using var enumerator = WaitEachAsync(batchId, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            RunResult result;
            try
            {
                if (!await enumerator.MoveNextAsync())
                {
                    break;
                }

                result = ToRunResult(enumerator.Current);
            }

            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                try
                {
                    await CancelBatchAsync(batchId, CancellationToken.None);
                }
                catch (Exception ex)
                {
                    Log.FailedToPropagateCancellation(logger, ex, batchId);
                }
                throw;
            }

            yield return result;
        }
    }

    public async Task<JobRun> WaitAsync(string runId, CancellationToken cancellationToken = default)
    {
        await foreach (var observation in ObserveAsync(runId, cancellationToken))
        {
            if (observation.Run.Status.IsTerminal)
            {
                return observation.Run;
            }
        }

        throw new InvalidOperationException($"Run '{runId}' observation ended before terminal state.");
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public async Task<T> WaitAsync<T>(string runId, CancellationToken cancellationToken = default)
    {
        if (TypeHelpers.TryGetAsyncEnumerableElementType(typeof(T), out var streamItemType))
        {
            var streamMethod = typeof(JobClient)
                .GetMethod(nameof(BuildObserverRunStreamResult), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(streamItemType);
            return (T)streamMethod.Invoke(this, [runId, cancellationToken])!;
        }

        var result = await WaitAsync(runId, cancellationToken);
        if (!result.IsSuccess)
        {
            throw new JobRunFailedException(result.Id, result.Status, result.Error);
        }

        if (result.Result is { } resultJson)
        {
            return ResultSerializer.Deserialize<T>(resultJson, options.SerializerOptions);
        }

        if (await TryBuildResultFromOutputEventsAsync<T>(result.Id, cancellationToken) is { } fromEvents)
        {
            return fromEvents;
        }

        throw new InvalidOperationException("Run did not produce a result.");
    }

    /// <inheritdoc />
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, CancellationToken cancellationToken = default) =>
        WaitStreamAsync<T>(runId, RunEventCursor.Start, cancellationToken);

    /// <inheritdoc />
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, RunEventCursor cursor,
        CancellationToken cancellationToken = default) =>
        StreamRunAsync<T>(runId, cursor, cancellationToken);

    /// <inheritdoc />
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId,
        CancellationToken cancellationToken = default) =>
        StreamEachAsync<T>(batchId, BatchEventCursor.Start, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public async IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId, BatchEventCursor cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var subscription = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        var sinceEventId = cursor.SinceEventId;
        var childSinceEventIds = new Dictionary<string, long>(cursor.ChildSinceEventIds, StringComparer.Ordinal);

        async Task<(bool EmittedAny, List<BatchStreamItem<T>> Items)> ReadBatchOutputEventsAsync()
        {
            var emittedAny = false;
            var items = new List<BatchStreamItem<T>>();
            while (true)
            {
                var events = await store.GetBatchOutputEventsAsync(batchId, sinceEventId, 200, cancellationToken);
                if (events.Count == 0)
                {
                    break;
                }

                foreach (var @event in events)
                {
                    if (@event.EventType != RunEventType.Output)
                    {
                        continue;
                    }

                    if (childSinceEventIds.TryGetValue(@event.RunId, out var childSinceId)
                        && @event.Id <= childSinceId)
                    {
                        continue;
                    }

                    sinceEventId = @event.Id;
                    childSinceEventIds[@event.RunId] = @event.Id;
                    emittedAny = true;
                    if (!TryDeserializeJson(
                            @event.Payload,
                            out T? item,
                            @event.RunId,
                            @event.Id,
                            @event.EventType,
                            "batch stream output"))
                    {
                        continue;
                    }

                    items.Add(new()
                    {
                        RunId = @event.RunId,
                        Item = item!,
                        Cursor = CreateBatchCursorSnapshot(sinceEventId, childSinceEventIds)
                    });
                }
            }

            return (emittedAny, items);
        }

        while (true)
        {
            var batch = await store.GetBatchAsync(batchId, cancellationToken);
            if (batch is null)
            {
                throw new InvalidOperationException($"Batch '{batchId}' was not found.");
            }

            var (emittedAny, batchItems) = await ReadBatchOutputEventsAsync();
            foreach (var item in batchItems)
            {
                yield return item;
            }

            if (batch.Status.IsTerminal)
            {
                var (terminalDrainEmitted, terminalDrainItems) = await ReadBatchOutputEventsAsync();
                foreach (var item in terminalDrainItems)
                {
                    yield return item;
                }

                if (!emittedAny && !terminalDrainEmitted)
                {
                    var graceDeadline = timeProvider.GetUtcNow() + GetTerminalEventGracePeriod();
                    while (timeProvider.GetUtcNow() < graceDeadline)
                    {
                        var remaining = graceDeadline - timeProvider.GetUtcNow();
                        if (remaining <= TimeSpan.Zero)
                        {
                            break;
                        }

                        await WaitForWakeupAsync(wakeup, remaining, cancellationToken);

                        var (graceDrainEmitted, graceDrainItems) = await ReadBatchOutputEventsAsync();
                        foreach (var item in graceDrainItems)
                        {
                            yield return item;
                        }

                        terminalDrainEmitted |= graceDrainEmitted;
                    }

                    if (!terminalDrainEmitted)
                    {
                        yield break;
                    }
                }
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
    }

    public async IAsyncEnumerable<JobRun> WaitEachAsync(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var batchEventSubscription = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);
        await using var batchTerminatedSubscription = await notifications.SubscribeAsync(
            NotificationChannels.BatchTerminated(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        DateTimeOffset? completedAfter = null;
        string? afterRunId = null;
        var observedTerminalRuns = 0;
        while (true)
        {
            var batch = await store.GetBatchAsync(batchId, cancellationToken);
            if (batch is null)
            {
                throw new InvalidOperationException($"Batch '{batchId}' was not found.");
            }

            var emittedAny = false;
            while (true)
            {
                var runs = await store.GetBatchTerminalRunsAsync(batchId, completedAfter, afterRunId, 200,
                    cancellationToken);
                if (runs.Count == 0)
                {
                    break;
                }

                emittedAny = true;
                foreach (var child in runs)
                {
                    if (child.CompletedAt is null)
                    {
                        continue;
                    }

                    completedAfter = child.CompletedAt;
                    afterRunId = child.Id;
                    observedTerminalRuns++;
                    yield return AttachSerializerOptions(child);
                }
            }

            if (batch.Status.IsTerminal)
            {
                var totalTerminal = batch.Succeeded + batch.Failed + batch.Cancelled;
                if ((observedTerminalRuns >= batch.Total || totalTerminal >= batch.Total) && !emittedAny)
                {
                    yield break;
                }
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
    }

    private IAsyncEnumerable<TItem> BuildObserverRunStreamResult<TItem>(string runId,
        CancellationToken cancellationToken) =>
        WaitStreamAsync<TItem>(runId, cancellationToken);

    private IAsyncEnumerable<TItem> BuildOwnedRunStreamResult<TItem>(string runId,
        CancellationToken ownerCancellationToken) =>
        OwnedRunStreamAsync<TItem>(runId, ownerCancellationToken);

    private async IAsyncEnumerable<TItem> OwnedRunStreamAsync<TItem>(string runId,
        CancellationToken ownerCancellationToken,
        [EnumeratorCancellation] CancellationToken consumerCancellationToken = default)
    {
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(
            ownerCancellationToken,
            consumerCancellationToken);

        await using var enumerator = WaitStreamAsync<TItem>(runId, linkedCts.Token)
            .GetAsyncEnumerator(linkedCts.Token);

        while (true)
        {
            TItem item;
            try
            {
                if (!await enumerator.MoveNextAsync())
                {
                    yield break;
                }

                item = enumerator.Current;
            }
            catch (OperationCanceledException) when (ownerCancellationToken.IsCancellationRequested)
            {
                await TryCancelOwnedRunAsync(runId);
                throw;
            }

            yield return item;
        }
    }

    private static BatchEventCursor CreateBatchCursorSnapshot(long sinceEventId,
        IReadOnlyDictionary<string, long> childSinceEventIds) =>
        new()
        {
            SinceEventId = sinceEventId,
            ChildSinceEventIds = new Dictionary<string, long>(childSinceEventIds, StringComparer.Ordinal)
        };

    private JobRun CreateRun(string jobName, string? serializedArguments, RunOptions runOptions,
        DateTimeOffset now, int priority, string? runId = null, string? rerunOfRunId = null)
    {
        var run = new JobRun
        {
            Id = runId ?? CreateRunId(),
            JobName = jobName,
            Status = JobStatus.Pending,
            Arguments = serializedArguments,
            CreatedAt = now,
            NotBefore = runOptions.NotBefore ?? now,
            NotAfter = runOptions.NotAfter,
            Priority = priority,
            QueuePriority = 0,
            DeduplicationId = runOptions.DeduplicationId,
            RerunOfRunId = rerunOfRunId,
            Progress = 0,
            Attempt = 0,
            TraceId = Activity.Current?.TraceId.ToString(),
            SpanId = Activity.Current?.SpanId.ToString()
        };

        return LinkToCurrentRunScope(run);
    }

    private static JobRun LinkToCurrentRunScope(JobRun run)
    {
        var current = JobContext.Current;
        if (current is null)
        {
            return run;
        }

        return run with
        {
            ParentRunId = run.ParentRunId ?? current.RunId,
            RootRunId = run.RootRunId ?? current.RootRunId
        };
    }

    private static string CreateRunId() => Guid.CreateVersion7().ToString("N");

    private JobRun AttachSerializerOptions(JobRun run) => run with { SerializerOptions = _serializerOptions };

    private RunResult ToRunResult(JobRun run) => new()
    {
        RunId = run.Id,
        JobName = run.JobName,
        Status = run.Status,
        Error = run.Error,
        ResultJson = run.Result,
        SerializerOptions = _serializerOptions
    };

    public async Task<JobBatch> WaitBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var subscription = await notifications.SubscribeAsync(
            NotificationChannels.BatchTerminated(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        while (true)
        {
            var batch = await store.GetBatchAsync(batchId, cancellationToken);
            if (batch is null)
                throw new InvalidOperationException($"Batch '{batchId}' was not found.");
            if (batch.Status.IsTerminal)
                return batch;
            await wakeup.WaitAsync(options.PollingInterval, cancellationToken);
        }
    }

    public IAsyncEnumerable<RunEvent> StreamBatchEventsAsync(string batchId, CancellationToken cancellationToken = default) =>
        StreamBatchEventsImpl(batchId, cancellationToken);

    private async IAsyncEnumerable<RunEvent> StreamRunEventsImpl(string runId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var obs in ObserveAsync(runId, cancellationToken))
        {
            if (obs.Event is { } e)
                yield return e;
            else if (obs.Run.Status.IsTerminal)
                yield break;
        }
    }

    public async Task<IReadOnlyList<JobRun>> GetBatchRunsAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var runs = new List<JobRun>();
        await foreach (var record in EnumerateBatchChildrenAsync(batchId, cancellationToken))
            runs.Add(record);
        return runs;
    }

    public IAsyncEnumerable<JobRun> StreamBatchRunsAsync(string batchId, CancellationToken cancellationToken = default) =>
        StreamBatchRunsImpl(batchId, cancellationToken);

    private async IAsyncEnumerable<JobRun> StreamBatchRunsImpl(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var result in WaitEachAsync(batchId, cancellationToken))
        {
            yield return result;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> StreamBatchResultsAsync<T>(string batchId, CancellationToken cancellationToken = default) =>
        StreamBatchResultsImpl<T>(batchId, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    private async IAsyncEnumerable<T> StreamBatchResultsImpl<T>(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in StreamEachAsync<T>(batchId, cancellationToken))
            yield return item.Item;
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    public async Task<T[]> GetBatchResultsAsync<T>(string batchId, CancellationToken cancellationToken = default)
    {
        var results = new List<RunResult>();
        await foreach (var result in WaitEachAsync(batchId, cancellationToken))
            results.Add(ToRunResult(result));
        return ConvertBatchResults<T>(results);
    }

    

    private async IAsyncEnumerable<RunEvent> StreamBatchEventsImpl(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var batchEventSubscription = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);
        await using var batchTerminatedSubscription = await notifications.SubscribeAsync(
            NotificationChannels.BatchTerminated(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        var sinceEventId = 0L;
        while (true)
        {
            var batch = await store.GetBatchAsync(batchId, cancellationToken);
            if (batch is null)
            {
                throw new InvalidOperationException($"Batch '{batchId}' was not found.");
            }

            var emittedAny = false;
            while (true)
            {
                var events = await store.GetBatchEventsAsync(batchId, sinceEventId, 200, cancellationToken);
                if (events.Count == 0)
                {
                    break;
                }

                emittedAny = true;
                foreach (var @event in events)
                {
                    sinceEventId = @event.Id;
                    yield return @event;
                }
            }

            if (batch.Status.IsTerminal)
            {
                var terminalDrainEmitted = false;
                while (true)
                {
                    var events = await store.GetBatchEventsAsync(batchId, sinceEventId, 200, cancellationToken);
                    if (events.Count == 0)
                    {
                        break;
                    }

                    terminalDrainEmitted = true;
                    foreach (var @event in events)
                    {
                        sinceEventId = @event.Id;
                        yield return @event;
                    }
                }

                if (!emittedAny && !terminalDrainEmitted)
                {
                    var graceDeadline = timeProvider.GetUtcNow() + GetTerminalEventGracePeriod();
                    while (timeProvider.GetUtcNow() < graceDeadline)
                    {
                        var remaining = graceDeadline - timeProvider.GetUtcNow();
                        if (remaining <= TimeSpan.Zero)
                        {
                            break;
                        }

                        await WaitForWakeupAsync(wakeup, remaining, cancellationToken);

                        while (true)
                        {
                            var events = await store.GetBatchEventsAsync(batchId, sinceEventId, 200, cancellationToken);
                            if (events.Count == 0)
                            {
                                break;
                            }

                            terminalDrainEmitted = true;
                            foreach (var @event in events)
                            {
                                sinceEventId = @event.Id;
                                yield return @event;
                            }
                        }
                    }

                    if (!terminalDrainEmitted)
                    {
                        yield break;
                    }
                }
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
    }

    private static Task ReleaseWakeupAsync(SemaphoreSlim wakeup) => WakeupSignal.ReleaseAsync(wakeup);

    private async Task WaitForWakeupAsync(SemaphoreSlim wakeup, CancellationToken cancellationToken)
    {
        await WakeupSignal.WaitAsync(wakeup, options.PollingInterval, cancellationToken);
    }

    private static TimeSpan GetTerminalEventGracePeriod(TimeSpan pollingInterval) =>
        pollingInterval <= TimeSpan.FromMilliseconds(100) ? pollingInterval : TimeSpan.FromMilliseconds(100);

    private TimeSpan GetTerminalEventGracePeriod() => GetTerminalEventGracePeriod(options.PollingInterval);

    private static Task WaitForWakeupAsync(SemaphoreSlim wakeup, TimeSpan timeout, CancellationToken cancellationToken) =>
        WakeupSignal.WaitAsync(wakeup, timeout, cancellationToken);

    private async IAsyncEnumerable<JobRun> EnumerateBatchChildrenAsync(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var skip = 0;
        const int pageSize = 200;
        while (true)
        {
            var page = await store.GetRunsAsync(new() { BatchId = batchId }, skip, pageSize, cancellationToken);
            if (page.Items.Count == 0)
            {
                yield break;
            }

            foreach (var child in page.Items)
            {
                yield return AttachSerializerOptions(child);
            }

            skip += page.Items.Count;
        }
    }

    private async IAsyncEnumerable<T> StreamRunAsync<T>(string runId, RunEventCursor cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var emittedOutput = false;
        await foreach (var observation in ObserveAsync(runId, cursor, cancellationToken))
        {
            if (observation.Event?.EventType == RunEventType.Output)
            {
                emittedOutput = true;
                if (TryDeserializeJson(
                        observation.Event.Payload,
                        out T? item,
                        observation.Run.Id,
                        observation.Event.Id,
                        observation.Event.EventType,
                        "run output stream"))
                {
                    yield return item!;
                }
            }

            if (observation.Event is { })
            {
                continue;
            }

            if (observation.Run.Status.IsTerminal)
            {
                if (observation.Run.Status == JobStatus.Failed)
                {
                    throw new JobRunFailedException(runId, observation.Run.Status, observation.Run.Error);
                }

                if (!emittedOutput && observation.Run.Result is { } resultJson
                                   && TryDeserializeList(resultJson, out List<T>? resultItems))
                {
                    foreach (var item in resultItems!)
                    {
                        yield return item;
                    }
                }

                yield break;
            }
        }
    }

    private async Task<int?> ResolveRequestedPriorityAsync(string jobName, int? explicitPriority,
        CancellationToken cancellationToken)
    {
        var job = await store.GetJobAsync(jobName, cancellationToken);
        if (job is null)
        {
            Log.TriggerRequestedForUnknownJob(logger, jobName);
        }

        return explicitPriority ?? job?.Priority;
    }

    private static T[] ConvertBatchResults<T>(IReadOnlyList<RunResult> results)
    {
        var values = new List<T>(results.Count);
        var exceptions = new List<Exception>();
        foreach (var result in results)
        {
            if (!result.IsSuccess)
            {
                exceptions.Add(new JobRunFailedException(result.RunId, result.Status, result.Error));
                continue;
            }

            values.Add(result.GetResult<T>());
        }

        if (exceptions.Count > 0)
        {
            throw new AggregateException(exceptions);
        }

        return values.ToArray();
    }

    private async Task CancelRunAndDescendantsAsync(string runId, ISet<string> visited,
        CancellationToken cancellationToken)
    {
        if (!visited.Add(runId))
        {
            return;
        }

        if (await store.TryCancelRunAsync(runId, cancellationToken))
        {
            await notifications.PublishAsync(NotificationChannels.RunCancel(runId), null, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunTerminated(runId), runId, cancellationToken);
        }

        var cancelledChildIds = await store.CancelChildRunsAsync(runId, cancellationToken);
        foreach (var childId in cancelledChildIds)
        {
            visited.Add(childId);
            await notifications.PublishAsync(NotificationChannels.RunCancel(childId), null, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunTerminated(childId), childId, cancellationToken);

            // Recurse into cancelled children to handle nested batch coordinators.
            await CancelRunAndDescendantsAsync(childId, visited, cancellationToken);
        }
    }

    private async Task<T?> TryBuildResultFromOutputEventsAsync<T>(string runId, CancellationToken cancellationToken)
    {
        var target = typeof(T);
        if (target.IsGenericType && target.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            var asyncItemType = target.GetGenericArguments()[0];
            var asyncMethod = typeof(JobClient)
                .GetMethod(nameof(BuildOutputAsyncEnumerable), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(asyncItemType);
            var stream = asyncMethod.Invoke(this, [runId]);
            return (T?)stream;
        }

        if (!TypeHelpers.TryGetCollectionElementType(target, out var itemType, out var asArray))
        {
            return default;
        }

        var method = typeof(JobClient)
            .GetMethod(nameof(MaterializeOutputCollectionAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(itemType);
        var materialized = await (Task<object?>)method.Invoke(this, [runId, cancellationToken])!;
        if (materialized is null)
        {
            return default;
        }

        if (asArray && materialized is System.Collections.IEnumerable enumerable)
        {
            var toArray = typeof(Enumerable)
                .GetMethod(nameof(Enumerable.ToArray))!
                .MakeGenericMethod(itemType);
            materialized = toArray.Invoke(null, [enumerable])!;
        }

        return (T)materialized;
    }

    private IAsyncEnumerable<TItem> BuildOutputAsyncEnumerable<TItem>(string runId) =>
        ReadOutputEventsAsync<TItem>(runId);

    private async IAsyncEnumerable<TItem> ReadOutputEventsAsync<TItem>(string runId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var events = await store.GetEventsAsync(runId, 0, [RunEventType.Output], cancellationToken: cancellationToken);
        foreach (var @event in events)
        {
            if (TryDeserializeJson(
                    @event.Payload,
                    out TItem? item,
                    runId,
                    @event.Id,
                    @event.EventType,
                    "output event replay"))
            {
                yield return item!;
            }
        }
    }

    private async Task<object?> MaterializeOutputCollectionAsync<TItem>(string runId,
        CancellationToken cancellationToken)
    {
        var events = await store.GetEventsAsync(runId, 0, [RunEventType.Output], cancellationToken: cancellationToken);
        if (events.Count == 0)
        {
            return null;
        }

        var list = new List<TItem>(events.Count);
        foreach (var @event in events)
        {
            if (!TryDeserializeJson(
                    @event.Payload,
                    out TItem? item,
                    runId,
                    @event.Id,
                    @event.EventType,
                    "output collection materialization"))
            {
                continue;
            }

            list.Add(item!);
        }

        return list.Count == 0 ? null : list;
    }

    private bool TryDeserializeList<T>(string json, out List<T>? items)
    {
        try
        {
            items = JsonSerializer.Deserialize<List<T>>(json, _serializerOptions);
            return items is { };
        }
        catch
        {
            items = null;
            return false;
        }
    }

    private bool TryDeserializeJson<T>(string json, out T? value, string runId, long eventId, RunEventType eventType,
        string operation)
    {
        try
        {
            value = JsonSerializer.Deserialize<T>(json, _serializerOptions);
            if (value is null)
            {
                Log.DeserializationReturnedNull(logger, runId, eventId, eventType, operation);
                return false;
            }

            return true;
        }
        catch (JsonException ex)
        {
            Log.DeserializationFailed(logger, ex, runId, eventId, eventType, operation);
            value = default;
            return false;
        }
    }

    private async Task<IReadOnlyList<RunEvent>> BuildClonedRunScopedInputEventsAsync(string sourceRunId,
        string destinationRunId,
        CancellationToken cancellationToken)
    {
        var inputEvents = await store.GetEventsAsync(
            sourceRunId,
            0,
            [RunEventType.InputDeclared, RunEventType.Input, RunEventType.InputComplete],
            cancellationToken: cancellationToken);

        if (inputEvents.Count == 0)
        {
            return [];
        }

        var now = timeProvider.GetUtcNow();
        var cloned = new List<RunEvent>(inputEvents.Count);
        foreach (var @event in inputEvents)
        {
            cloned.Add(new()
            {
                RunId = destinationRunId,
                EventType = @event.EventType,
                Payload = @event.Payload,
                CreatedAt = now,
                Attempt = 0
            });
        }

        return cloned;
    }

    private PreparedArguments PrepareArguments(object? args)
    {
        if (args is null)
        {
            return PreparedArguments.Empty;
        }

        if (TryCreateStreamSource("$root", args, out var rootStream))
        {
            return new(
                null,
                [rootStream!],
                new() { Arguments = ["$root"] });
        }

        var properties = args.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanRead && p.GetIndexParameters().Length == 0)
            .ToArray();
        if (properties.Length == 0)
        {
            return new(JsonSerializer.Serialize(args, _serializerOptions), []);
        }

        var streamSources = new List<StreamingArgumentSource>();
        var payload = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in properties)
        {
            var value = property.GetValue(args);
            if (TryCreateStreamSource(property.Name, value, out var streamSource))
            {
                streamSources.Add(streamSource!);
                continue;
            }

            payload[property.Name] = value;
        }

        if (streamSources.Count == 0)
        {
            return new(JsonSerializer.Serialize(args, _serializerOptions), []);
        }

        var serializedPayload = payload.Count == 0
            ? null
            : JsonSerializer.Serialize(payload, _serializerOptions);

        return new(
            serializedPayload,
            streamSources,
            new() { Arguments = streamSources.Select(s => s.Argument).ToArray() });
    }

    private IReadOnlyList<RunEvent> BuildInitialEvents(string runId, InputDeclarationEnvelope? declaration)
    {
        if (declaration is null)
        {
            return [];
        }

        return
        [
            new()
            {
                RunId = runId,
                EventType = RunEventType.InputDeclared,
                Payload = JsonSerializer.Serialize(declaration, _serializerOptions),
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = 0
            }
        ];
    }

    private static bool TryCreateStreamSource(string argumentName, object? value,
        out StreamingArgumentSource? source)
    {
        if (value is { } && TypeHelpers.TryGetAsyncEnumerableElementType(value.GetType(), out var elementType))
        {
            var boxMethod = typeof(JobClient)
                .GetMethod(nameof(BoxAsyncEnumerable), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(elementType);
            var boxed = (IAsyncEnumerable<object?>)boxMethod.Invoke(null, [value])!;
            source = new(argumentName, boxed);
            return true;
        }

        source = null;
        return false;
    }

    private static async IAsyncEnumerable<object?> BoxAsyncEnumerable<T>(IAsyncEnumerable<T> stream)
    {
        await foreach (var item in stream)
        {
            yield return item;
        }
    }

    private void StartInputPump(string runId, IReadOnlyList<StreamingArgumentSource> streams)
    {
        var pumpCts = new CancellationTokenSource();
        if (!_inputPumpTokens.TryAdd(runId, pumpCts))
        {
            pumpCts.Dispose();
            throw new InvalidOperationException($"An input pump is already active for run '{runId}'.");
        }

        _ = MonitorInputPumpAsync(runId, streams, pumpCts);
    }

    private async Task MonitorInputPumpAsync(string runId, IReadOnlyList<StreamingArgumentSource> streams,
        CancellationTokenSource pumpCts)
    {
        try
        {
            await using var terminatedSubscription = await notifications.SubscribeAsync(
                NotificationChannels.RunTerminated(runId),
                _ =>
                {
                    pumpCts.Cancel();
                    return Task.CompletedTask;
                },
                CancellationToken.None);
            await using var cancelSubscription = await notifications.SubscribeAsync(
                NotificationChannels.RunCancel(runId),
                _ =>
                {
                    pumpCts.Cancel();
                    return Task.CompletedTask;
                },
                CancellationToken.None);

            await PumpInputStreamsAsync(runId, streams, pumpCts.Token);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not AccessViolationException)
        {
            Log.InputStreamingFailed(logger, ex, runId);
            await TryCancelRunBestEffortAsync(runId);
        }
        finally
        {
            if (_inputPumpTokens.TryRemove(runId, out var activePump))
            {
                activePump.Dispose();
            }
            else
            {
                pumpCts.Dispose();
            }
        }
    }

    private async Task PumpInputStreamsAsync(string runId, IReadOnlyList<StreamingArgumentSource> streams,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.WhenAll(streams.Select(stream => PumpSingleInputStreamAsync(runId, stream, cancellationToken)));
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
        }
    }

    private async Task PumpSingleInputStreamAsync(string runId, StreamingArgumentSource stream,
        CancellationToken cancellationToken)
    {
        long sequence = 0;
        try
        {
            await foreach (var item in stream.Stream.WithCancellation(cancellationToken))
            {
                sequence++;
                await AppendInputEventAsync(runId, RunEventType.Input, new()
                {
                    Argument = stream.Argument,
                    Sequence = sequence,
                    Payload = JsonSerializer.Serialize(item, _serializerOptions),
                    IsComplete = false,
                    Error = null
                }, cancellationToken);
            }

            await AppendInputEventAsync(runId, RunEventType.InputComplete, new()
            {
                Argument = stream.Argument,
                Sequence = sequence + 1,
                Payload = null,
                IsComplete = true,
                Error = null
            }, CancellationToken.None);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await AppendInputEventAsync(runId, RunEventType.InputComplete, new()
            {
                Argument = stream.Argument,
                Sequence = sequence + 1,
                Payload = null,
                IsComplete = true,
                Error = null
            }, CancellationToken.None);
        }
        catch (OperationCanceledException ex)
        {
            await AppendInputEventAsync(runId, RunEventType.InputComplete, new()
            {
                Argument = stream.Argument,
                Sequence = sequence + 1,
                Payload = null,
                IsComplete = true,
                Error = ex.Message
            }, CancellationToken.None);
        }
        catch (Exception ex)
        {
            await AppendInputEventAsync(runId, RunEventType.InputComplete, new()
            {
                Argument = stream.Argument,
                Sequence = sequence + 1,
                Payload = null,
                IsComplete = true,
                Error = ex.Message
            }, CancellationToken.None);

            throw;
        }
    }

    private async Task AppendInputEventAsync(string runId, RunEventType eventType, InputEnvelope payload,
        CancellationToken cancellationToken)
    {
        await store.AppendEventsAsync([
            new()
            {
                RunId = runId,
                EventType = eventType,
                Payload = JsonSerializer.Serialize(payload, _serializerOptions),
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = 0
            }
        ], cancellationToken);

        await notifications.PublishAsync(NotificationChannels.RunInput(runId), runId, cancellationToken);
    }

    private async Task TryCancelOwnedRunAsync(string runId)
    {
        try
        {
            await CancelAsync(runId, CancellationToken.None);
        }
        catch (Exception ex)
        {
            Log.FailedToPropagateCancellation(logger, ex, runId);
        }
    }

    private async Task TryCancelRunBestEffortAsync(string runId)
    {
        try
        {
            await CancelAsync(runId, CancellationToken.None);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not AccessViolationException)
        {
            Log.FailedBestEffortCancellation(logger, ex, runId);
        }
    }

    private static partial class Log
    {
        [LoggerMessage(EventId = 1001, Level = LogLevel.Warning,
            Message = "Trigger requested for unknown job '{JobName}'.")]
        public static partial void TriggerRequestedForUnknownJob(ILogger logger, string jobName);

        [LoggerMessage(EventId = 1002, Level = LogLevel.Warning,
            Message = "Input streaming failed for run '{RunId}'.")]
        public static partial void InputStreamingFailed(ILogger logger, Exception exception, string runId);

        [LoggerMessage(EventId = 1004, Level = LogLevel.Warning,
            Message = "Failed to propagate cancellation for run '{RunId}'.")]
        public static partial void FailedToPropagateCancellation(ILogger logger, Exception exception, string runId);

        [LoggerMessage(EventId = 1005, Level = LogLevel.Warning,
            Message =
                "Failed to deserialize payload for run '{RunId}', event '{EventId}' ({EventType}) during {Operation}.")]
        public static partial void DeserializationFailed(ILogger logger, Exception exception, string runId,
            long eventId, RunEventType eventType, string operation);

        [LoggerMessage(EventId = 1006, Level = LogLevel.Warning,
            Message =
                "Deserializer returned null for run '{RunId}', event '{EventId}' ({EventType}) during {Operation}.")]
        public static partial void DeserializationReturnedNull(ILogger logger, string runId, long eventId,
            RunEventType eventType, string operation);

        [LoggerMessage(EventId = 1007, Level = LogLevel.Warning,
            Message = "Best-effort cancellation failed for run '{RunId}'.")]
        public static partial void FailedBestEffortCancellation(ILogger logger, Exception exception, string runId);
    }

    private sealed record PreparedArguments(
        string? SerializedArguments,
        IReadOnlyList<StreamingArgumentSource> Streams,
        InputDeclarationEnvelope? StreamDeclaration = null)
    {
        public static PreparedArguments Empty { get; } = new(null, []);
    }

    private sealed record StreamingArgumentSource(string Argument, IAsyncEnumerable<object?> Stream);

}
