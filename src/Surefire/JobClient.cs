using System.Diagnostics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Surefire;

/// <summary>
///     Default implementation of <see cref="IJobClient" /> built on top of the store and notification contracts.
/// </summary>
public sealed partial class JobClient(
    IJobStore store,
    INotificationProvider notifications,
    TimeProvider timeProvider,
    SurefireOptions options,
    ILogger<JobClient> logger) : IJobClient
{
    private readonly JsonSerializerOptions _serializerOptions = options.SerializerOptions;

    /// <inheritdoc />
    public Task<string> TriggerAsync(string jobName, CancellationToken cancellationToken = default) =>
        TriggerAsync(jobName, null, new(), cancellationToken);

    /// <inheritdoc />
    public Task<string> TriggerAsync(string jobName, object? args, CancellationToken cancellationToken = default) =>
        TriggerAsync(jobName, args, new(), cancellationToken);

    /// <inheritdoc />
    public async Task<string> TriggerAsync(string jobName, object? args, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        var prepared = PrepareArguments(args);
        var requestedPriority = await ResolveRequestedPriorityAsync(jobName, options.Priority, cancellationToken);
        var run = CreateRun(jobName, prepared.SerializedArguments, options, timeProvider.GetUtcNow(),
            requestedPriority ?? 0);
        var initialEvents = BuildInitialEvents(run.Id, prepared.StreamDeclaration);
        var created = await store.TryCreateRunAsync(
            run,
            initialEvents: initialEvents,
            cancellationToken: cancellationToken);
        if (!created)
        {
            throw new InvalidOperationException($"Run creation for job '{jobName}' was rejected.");
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);

        if (prepared.Streams.Count > 0)
        {
            // Input pumping must continue after acceptance even if the caller/request token is canceled.
            _ = PumpInputStreamsAsync(run.Id, prepared.Streams, CancellationToken.None);
        }

        return run.Id;
    }

    /// <inheritdoc />
    public Task<RunResult> RunAsync(string jobName, CancellationToken cancellationToken = default) =>
        RunAsync(jobName, null, new(), cancellationToken);

    /// <inheritdoc />
    public Task<RunResult> RunAsync(string jobName, object? args, CancellationToken cancellationToken = default) =>
        RunAsync(jobName, args, new(), cancellationToken);

    /// <inheritdoc />
    public async Task<RunResult> RunAsync(string jobName, object? args, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        var runId = await TriggerAsync(jobName, args, options, cancellationToken);
        try
        {
            return await WaitAsync(runId, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryCancelOwnedRunAsync(runId);
            throw;
        }
    }

    /// <inheritdoc />
    public Task<T> RunAsync<T>(string jobName, CancellationToken cancellationToken = default) =>
        RunAsync<T>(jobName, null, new(), cancellationToken);

    /// <inheritdoc />
    public Task<T> RunAsync<T>(string jobName, object? args, CancellationToken cancellationToken = default) =>
        RunAsync<T>(jobName, args, new(), cancellationToken);

    /// <inheritdoc />
    public async Task<T> RunAsync<T>(string jobName, object? args, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        var runId = await TriggerAsync(jobName, args, options, cancellationToken);
        if (TryGetAsyncEnumerableElementType(typeof(T), out var streamItemType))
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await TryCancelOwnedRunAsync(runId);
                cancellationToken.ThrowIfCancellationRequested();
            }

            var streamMethod = typeof(JobClient)
                .GetMethod(nameof(BuildOwnedRunStreamResult), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(streamItemType);
            return (T)streamMethod.Invoke(this, [runId, cancellationToken])!;
        }

        try
        {
            return await WaitAsync<T>(runId, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryCancelOwnedRunAsync(runId);
            throw;
        }
    }

    /// <inheritdoc />
    public IAsyncEnumerable<T> StreamAsync<T>(string jobName, CancellationToken cancellationToken = default) =>
        StreamAsync<T>(jobName, null, new(), cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<T> StreamAsync<T>(string jobName, object? args,
        CancellationToken cancellationToken = default) =>
        StreamAsync<T>(jobName, args, new(), cancellationToken);

    /// <inheritdoc />
    public async IAsyncEnumerable<T> StreamAsync<T>(string jobName, object? args, RunOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var runId = await TriggerAsync(jobName, args, options, cancellationToken);
        await using var enumerator = WaitStreamAsync<T>(runId, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);

        while (true)
        {
            T item;
            try
            {
                if (!await enumerator.MoveNextAsync())
                {
                    break;
                }

                item = enumerator.Current;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await TryCancelOwnedRunAsync(runId);
                throw;
            }

            yield return item;
        }
    }

    /// <inheritdoc />
    public IAsyncEnumerable<RunObservation> ObserveAsync(string runId,
        CancellationToken cancellationToken = default) =>
        ObserveAsync(runId, RunEventCursor.Start, cancellationToken);

    /// <inheritdoc />
    public async IAsyncEnumerable<RunObservation> ObserveAsync(string runId, RunEventCursor cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var eventSub = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(runId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);
        await using var completionSub = await notifications.SubscribeAsync(
            NotificationChannels.RunCompleted(runId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        var sinceId = cursor.SinceEventId;
        while (true)
        {
            var run = await store.GetRunAsync(runId, cancellationToken);
            if (run is null)
            {
                throw new InvalidOperationException($"Run '{runId}' was not found.");
            }

            var events = await store.GetEventsAsync(runId, sinceId, null, null, cancellationToken);
            foreach (var @event in events)
            {
                sinceId = @event.Id;
                yield return new()
                {
                    Run = run,
                    Event = @event,
                    Cursor = new() { SinceEventId = sinceId }
                };
            }

            if (run.Status.IsTerminal)
            {
                yield return new()
                {
                    Run = run,
                    Event = null,
                    Cursor = new() { SinceEventId = sinceId }
                };
                yield break;
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
    }

    /// <inheritdoc />
    public Task<string> TriggerAllAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default) =>
        TriggerAllAsync(jobName, argsList, new(), cancellationToken);

    /// <inheritdoc />
    public async Task<string> TriggerAllAsync(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        var requestedPriority = await ResolveRequestedPriorityAsync(jobName, options.Priority, cancellationToken);

        var args = argsList;
        if (!args.TryGetNonEnumeratedCount(out var argsCount))
        {
            var materializedArgs = argsList.ToArray();
            args = materializedArgs;
            argsCount = materializedArgs.Length;
        }

        var now = timeProvider.GetUtcNow();
        var coordinatorId = CreateRunId();
        var coordinator = CreateRun(jobName, null, options, now, requestedPriority ?? 0, coordinatorId);
        coordinator.Status = JobStatus.Running;
        coordinator.Attempt = 0;
        coordinator.BatchTotal = argsCount;
        coordinator.BatchCompleted = 0;
        coordinator.BatchFailed = 0;

        var runs = new List<JobRun>(argsCount + 1) { coordinator };
        var streamPumps = new List<(string RunId, IReadOnlyList<StreamingArgumentSource> Streams,
            InputDeclarationEnvelope Declaration)>();
        foreach (var item in args)
        {
            var prepared = PrepareArguments(item);
            var child = CreateRun(jobName, prepared.SerializedArguments, options, now, requestedPriority ?? 0);
            child.ParentRunId = coordinatorId;
            child.RootRunId = coordinator.RootRunId ?? coordinatorId;
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

        await store.CreateRunsAsync(
            runs,
            initialEvents,
            cancellationToken);

        await notifications.PublishAsync(NotificationChannels.RunCreated, coordinatorId, cancellationToken);

        if (argsCount == 0)
        {
            await TryCompleteEmptyBatchCoordinatorAsync(coordinatorId, cancellationToken);
            return coordinatorId;
        }

        foreach (var (runId, streams, _) in streamPumps)
        {
            // Child input streams are part of accepted work and should not be tied to request lifetime.
            _ = PumpInputStreamsAsync(runId, streams, CancellationToken.None);
        }

        return coordinatorId;
    }

    /// <inheritdoc />
    public async Task<RunResult[]> RunAllAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default)
    {
        var results = new List<RunResult>();
        await foreach (var result in RunEachAsync(jobName, argsList, cancellationToken))
        {
            results.Add(result);
        }

        return results.ToArray();
    }

    /// <inheritdoc />
    public async Task<RunResult[]> RunAllAsync(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        var results = new List<RunResult>();
        await foreach (var result in RunEachAsync(jobName, argsList, options, cancellationToken))
        {
            results.Add(result);
        }

        return results.ToArray();
    }

    /// <inheritdoc />
    public async Task<T[]> RunAllAsync<T>(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default)
    {
        var results = await RunAllAsync(jobName, argsList, cancellationToken);
        return ConvertBatchResults<T>(results);
    }

    /// <inheritdoc />
    public async Task<T[]> RunAllAsync<T>(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default)
    {
        var results = await RunAllAsync(jobName, argsList, options, cancellationToken);
        return ConvertBatchResults<T>(results);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RunResult> RunEachAsync(string jobName, IEnumerable<object?> argsList,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var batchId = await TriggerAllAsync(jobName, argsList, cancellationToken);
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

                result = enumerator.Current;
            }

            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await TryCancelOwnedRunAsync(batchId);
                throw;
            }

            yield return result;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RunResult> RunEachAsync(string jobName, IEnumerable<object?> argsList,
        RunOptions options,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var batchId = await TriggerAllAsync(jobName, argsList, options, cancellationToken);
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

                result = enumerator.Current;
            }

            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await TryCancelOwnedRunAsync(batchId);
                throw;
            }

            yield return result;
        }
    }

    /// <inheritdoc />
    public async Task<RunResult> WaitAsync(string runId, CancellationToken cancellationToken = default)
    {
        await foreach (var observation in ObserveAsync(runId, cancellationToken))
        {
            if (observation.Run.Status.IsTerminal)
            {
                return ToRunResult(observation.Run);
            }
        }

        throw new InvalidOperationException($"Run '{runId}' observation ended before terminal state.");
    }

    /// <inheritdoc />
    public async Task<T> WaitAsync<T>(string runId, CancellationToken cancellationToken = default)
    {
        if (TryGetAsyncEnumerableElementType(typeof(T), out var streamItemType))
        {
            var streamMethod = typeof(JobClient)
                .GetMethod(nameof(BuildObserverRunStreamResult), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(streamItemType);
            return (T)streamMethod.Invoke(this, [runId, cancellationToken])!;
        }

        var result = await WaitAsync(runId, cancellationToken);
        if (!result.IsSuccess)
        {
            throw new JobRunFailedException(result.RunId, result.Error);
        }

        if (result.TryGetResult<T>(out var typed))
        {
            return typed;
        }

        if (await TryBuildResultFromOutputEventsAsync<T>(result.RunId, cancellationToken) is { } fromEvents)
        {
            return fromEvents;
        }

        return result.GetResult<T>();
    }

    /// <inheritdoc />
    public IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, CancellationToken cancellationToken = default) =>
        WaitStreamAsync<T>(runId, RunEventCursor.Start, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, RunEventCursor cursor,
        CancellationToken cancellationToken = default) =>
        StreamRunAsync<T>(runId, cursor, cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId,
        CancellationToken cancellationToken = default) =>
        StreamEachAsync<T>(batchId, BatchRunEventCursor.Start, cancellationToken);

    /// <inheritdoc />
    public async IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId, BatchRunEventCursor cursor,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var subscription = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        var sinceEventId = cursor.SinceEventId;
        var childSinceEventIds = new Dictionary<string, long>(cursor.ChildSinceEventIds, StringComparer.Ordinal);

        while (true)
        {
            var coordinator = await store.GetRunAsync(batchId, cancellationToken);
            if (coordinator is null)
            {
                throw new InvalidOperationException($"Batch run '{batchId}' was not found.");
            }

            var emittedAny = false;
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

                    yield return new()
                    {
                        RunId = @event.RunId,
                        Item = item!,
                        Cursor = CreateBatchCursorSnapshot(sinceEventId, childSinceEventIds)
                    };
                }
            }

            if (coordinator.Status.IsTerminal)
            {
                if (!coordinator.BatchTotal.HasValue || !emittedAny)
                {
                    yield break;
                }
            }

            await wakeup.WaitAsync(options.PollingInterval, cancellationToken);
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<RunResult> WaitEachAsync(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        await using var subscription = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        var cursorTime = (DateTimeOffset?)null;
        var cursorId = (string?)null;
        var emittedChildIds = new HashSet<string>(StringComparer.Ordinal);
        while (true)
        {
            var coordinator = await store.GetRunAsync(batchId, cancellationToken);
            if (coordinator is null)
            {
                throw new InvalidOperationException($"Batch run '{batchId}' was not found.");
            }

            var progressed = false;
            var candidates = new List<JobRun>();
            await foreach (var child in EnumerateBatchChildrenAsync(batchId, cancellationToken))
            {
                if (!child.Status.IsTerminal || child.CompletedAt is null)
                {
                    continue;
                }

                candidates.Add(child);
            }

            // Deterministic ordering contract: completion time first, then run id as stable tie-breaker.
            foreach (var child in candidates
                         .OrderBy(c => c.CompletedAt ?? DateTimeOffset.MinValue)
                         .ThenBy(c => c.Id, StringComparer.Ordinal))
            {
                if (cursorTime == child.CompletedAt
                    && cursorId is { } && string.CompareOrdinal(child.Id, cursorId) <= 0)
                {
                    continue;
                }

                if (!emittedChildIds.Add(child.Id))
                {
                    continue;
                }

                progressed = true;
                cursorTime = child.CompletedAt;
                cursorId = child.Id;
                yield return ToRunResult(child);
            }

            if (coordinator.Status.IsTerminal)
            {
                if (coordinator.BatchTotal is { } batchTotal)
                {
                    var terminalChildren = (coordinator.BatchCompleted ?? 0) + (coordinator.BatchFailed ?? 0);
                    if (terminalChildren >= batchTotal || emittedChildIds.Count >= batchTotal)
                    {
                        yield break;
                    }
                }
                else if (!progressed)
                {
                    yield break;
                }
            }

            await wakeup.WaitAsync(options.PollingInterval, cancellationToken);
        }
    }

    /// <inheritdoc />
    public async Task CancelAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        if (run.Status.IsTerminal)
        {
            throw new RunConflictException($"Run '{runId}' is already in a terminal state.");
        }

        await CancelRunAndDescendantsAsync(runId, new HashSet<string>(StringComparer.Ordinal), cancellationToken);
    }

    /// <inheritdoc />
    public async Task<string> RerunAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        if (run.BatchTotal.HasValue)
        {
            var children = new List<JobRun>();
            await foreach (var child in EnumerateBatchChildrenAsync(runId, cancellationToken))
            {
                children.Add(child);
            }

            var now = timeProvider.GetUtcNow();
            var requestedPriority = await ResolveRequestedPriorityAsync(run.JobName, null, cancellationToken);

            var rerunCoordinatorId = CreateRunId();
            var rerunCoordinator = CreateRun(run.JobName, null, new(), now, requestedPriority ?? 0,
                rerunCoordinatorId);
            rerunCoordinator.Status = JobStatus.Running;
            rerunCoordinator.Attempt = 0;
            rerunCoordinator.BatchTotal = children.Count;
            rerunCoordinator.BatchCompleted = 0;
            rerunCoordinator.BatchFailed = 0;

            var newChildren = new List<JobRun>(children.Count);
            foreach (var originalRun in children)
            {
                var child = CreateRun(
                    run.JobName,
                    originalRun.Arguments,
                    new(),
                    now,
                    requestedPriority ?? 0);
                child.ParentRunId = rerunCoordinatorId;
                child.RootRunId = rerunCoordinator.RootRunId ?? rerunCoordinatorId;
                newChildren.Add(child);
            }

            var initialEvents = new List<RunEvent>();
            for (var i = 0; i < children.Count; i++)
            {
                var cloned = await BuildClonedRunScopedInputEventsAsync(children[i].Id, newChildren[i].Id,
                    cancellationToken);
                initialEvents.AddRange(cloned);
            }

            var runs = new List<JobRun>(newChildren.Count + 1) { rerunCoordinator };
            runs.AddRange(newChildren);

            await store.CreateRunsAsync(runs, initialEvents, cancellationToken);

            if (children.Count == 0)
            {
                await notifications.PublishAsync(NotificationChannels.RunCreated, rerunCoordinatorId,
                    cancellationToken);
                await TryCompleteEmptyBatchCoordinatorAsync(rerunCoordinatorId, cancellationToken);
                return rerunCoordinatorId;
            }

            await notifications.PublishAsync(NotificationChannels.RunCreated, rerunCoordinatorId, cancellationToken);
            return rerunCoordinatorId;
        }

        var priority = await ResolveRequestedPriorityAsync(run.JobName, null, cancellationToken);
        var rerun = CreateRun(
            run.JobName,
            run.Arguments,
            new(),
            timeProvider.GetUtcNow(),
            priority ?? 0);

        var clonedInputEvents = await BuildClonedRunScopedInputEventsAsync(runId, rerun.Id, cancellationToken);

        var created = await store.TryCreateRunAsync(
            rerun,
            initialEvents: clonedInputEvents,
            cancellationToken: cancellationToken);
        if (!created)
        {
            throw new RunConflictException($"Run creation for rerun of '{runId}' was rejected.");
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, rerun.Id, cancellationToken);
        return rerun.Id;
    }

    /// <inheritdoc />
    public Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default) =>
        store.GetRunAsync(runId, cancellationToken);

    /// <inheritdoc />
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
                yield return run;
            }

            skip += page.Items.Count;
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

    private static BatchRunEventCursor CreateBatchCursorSnapshot(long sinceEventId,
        IReadOnlyDictionary<string, long> childSinceEventIds) =>
        new()
        {
            SinceEventId = sinceEventId,
            ChildSinceEventIds = new Dictionary<string, long>(childSinceEventIds, StringComparer.Ordinal)
        };

    private JobRun CreateRun(string jobName, string? serializedArguments, RunOptions runOptions,
        DateTimeOffset now, int priority, string? runId = null)
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
            Progress = 0,
            Attempt = 0,
            TraceId = Activity.Current?.TraceId.ToString(),
            SpanId = Activity.Current?.SpanId.ToString()
        };

        LinkToCurrentRunScope(run);
        return run;
    }

    private static void LinkToCurrentRunScope(JobRun run)
    {
        var current = JobContext.Current;
        if (current is null)
        {
            return;
        }

        if (run.ParentRunId is null)
        {
            run.ParentRunId = current.RunId;
        }

        if (run.RootRunId is null)
        {
            run.RootRunId = current.RootRunId;
        }
    }

    private static string CreateRunId() => Guid.CreateVersion7().ToString("N");

    private RunResult ToRunResult(JobRun run) => new()
    {
        RunId = run.Id,
        JobName = run.JobName,
        Status = run.Status,
        Error = run.Error,
        ResultJson = run.Result,
        SerializerOptions = _serializerOptions
    };

    private static async Task ReleaseWakeupAsync(SemaphoreSlim wakeup)
    {
        try
        {
            wakeup.Release();
        }
        catch (SemaphoreFullException)
        {
        }

        await Task.CompletedTask;
    }

    private async Task WaitForWakeupAsync(SemaphoreSlim wakeup, CancellationToken cancellationToken)
    {
        await wakeup.WaitAsync(options.PollingInterval, cancellationToken);
    }

    private async IAsyncEnumerable<JobRun> EnumerateBatchChildrenAsync(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var skip = 0;
        const int pageSize = 200;
        while (true)
        {
            var page = await store.GetRunsAsync(new() { ParentRunId = batchId }, skip, pageSize, cancellationToken);
            if (page.Items.Count == 0)
            {
                yield break;
            }

            foreach (var child in page.Items)
            {
                yield return child;
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
                if (observation.Run.Status == JobStatus.DeadLetter)
                {
                    throw new JobRunFailedException(runId, observation.Run.Error);
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
                exceptions.Add(new JobRunFailedException(result.RunId, result.Error));
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
            await notifications.PublishAsync(NotificationChannels.RunCompleted(runId), runId, cancellationToken);
        }

        await foreach (var child in EnumerateBatchChildrenAsync(runId, cancellationToken))
        {
            await CancelRunAndDescendantsAsync(child.Id, visited, cancellationToken);
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

        var itemType = TryGetCollectionElementType(target);
        if (itemType is null)
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

        return (T)materialized;
    }

    private IAsyncEnumerable<TItem> BuildOutputAsyncEnumerable<TItem>(string runId) =>
        ReadOutputEventsAsync<TItem>(runId);

    private async IAsyncEnumerable<TItem> ReadOutputEventsAsync<TItem>(string runId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var events = await store.GetEventsAsync(runId, 0, [RunEventType.Output], null, cancellationToken);
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
        var events = await store.GetEventsAsync(runId, 0, [RunEventType.Output], null, cancellationToken);
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

    private static Type? TryGetCollectionElementType(Type targetType)
    {
        if (targetType.IsGenericType)
        {
            var genericDefinition = targetType.GetGenericTypeDefinition();
            if (genericDefinition == typeof(List<>)
                || genericDefinition == typeof(IReadOnlyList<>)
                || genericDefinition == typeof(IList<>)
                || genericDefinition == typeof(IEnumerable<>))
            {
                return targetType.GetGenericArguments()[0];
            }
        }

        return null;
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
            null,
            cancellationToken);

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
        if (value is { } && TryGetAsyncEnumerableElementType(value.GetType(), out var elementType))
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

    private static bool TryGetAsyncEnumerableElementType(Type type, out Type elementType)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        var asyncEnumerable = type.GetInterfaces().FirstOrDefault(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>));
        if (asyncEnumerable is { })
        {
            elementType = asyncEnumerable.GetGenericArguments()[0];
            return true;
        }

        elementType = null!;
        return false;
    }

    private static async IAsyncEnumerable<object?> BoxAsyncEnumerable<T>(IAsyncEnumerable<T> stream)
    {
        await foreach (var item in stream)
        {
            yield return item;
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
        catch (Exception ex)
        {
            Log.InputStreamingFailed(logger, ex, runId);
            await TryCancelRunBestEffortAsync(runId);
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

    private async Task TryCompleteEmptyBatchCoordinatorAsync(string coordinatorId, CancellationToken cancellationToken)
    {
        var coordinator = await store.GetRunAsync(coordinatorId, cancellationToken);
        if (coordinator is null || coordinator.BatchTotal != 0)
        {
            return;
        }

        var completedAt = timeProvider.GetUtcNow();
        var transition = RunStatusTransition.RunningToCompleted(
            coordinatorId,
            coordinator.Attempt,
            completedAt,
            coordinator.NotBefore,
            coordinator.NodeName,
            1,
            null,
            null,
            coordinator.StartedAt,
            completedAt);

        if (!await store.TryTransitionRunAsync(transition, cancellationToken))
        {
            return;
        }

        await notifications.PublishAsync(NotificationChannels.RunCompleted(coordinatorId), coordinatorId,
            cancellationToken);
        await notifications.PublishAsync(NotificationChannels.RunEvent(coordinatorId), coordinatorId,
            cancellationToken);
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
        catch
        {
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
    }

    private sealed record PreparedArguments(
        string? SerializedArguments,
        IReadOnlyList<StreamingArgumentSource> Streams,
        InputDeclarationEnvelope? StreamDeclaration = null)
    {
        public static PreparedArguments Empty { get; } = new(null, []);
    }

    private sealed record StreamingArgumentSource(string Argument, IAsyncEnumerable<object?> Stream);

    private sealed class InputDeclarationEnvelope
    {
        public required string[] Arguments { get; init; }
    }

    private sealed class InputEnvelope
    {
        public required string Argument { get; init; }
        public required long Sequence { get; init; }
        public string? Payload { get; init; }
        public required bool IsComplete { get; init; }
        public string? Error { get; init; }
    }
}