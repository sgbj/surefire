using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
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
    BatchedEventWriter eventWriter,
    RunCancellationCoordinator runCancellation,
    TimeProvider timeProvider,
    SurefireOptions options,
    ILogger<JobClient> logger) : IJobClient
{
    private const int BatchFetchWindowSize = 64;
    private const int BatchEventPageSize = 200;
    private const string ClientCancellationReason = "Canceled by client request.";
    private const string OwnedOperationCancellationReason = "Canceled because the owning operation was canceled.";
    private static readonly TimeSpan BatchFetchWindowDelay = TimeSpan.FromMilliseconds(10);

    private readonly ConcurrentDictionary<string, CancellationTokenSource> _inputPumpTokens =
        new(StringComparer.Ordinal);

    private readonly JsonSerializerOptions _serializerOptions = options.SerializerOptions;

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public Task<JobRun> TriggerAsync(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default)
        => TriggerAsync(job, AsRunArguments(args), options, cancellationToken);

    public Task<JobBatch> TriggerBatchAsync(IEnumerable<BatchItem> runs, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(runs);
        return TriggerBatchHeterogeneousAsync(runs, cancellationToken);
    }

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public Task<JobBatch> TriggerBatchAsync(string job, IEnumerable<object?> args, BatchRunOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        return TriggerBatchAsync(job, args.Select(AsRunArguments), options, cancellationToken);
    }

    public Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default)
        => store.GetRunAsync(runId, cancellationToken);

    public async IAsyncEnumerable<JobRun> GetRunsAsync(RunFilter filter,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Snapshot the upper bound so concurrent inserts don't shift our pagination window.
        // AddTicks(1) keeps runs created at the current instant in scope.
        var snapshotCreatedBefore = filter.CreatedBefore ?? timeProvider.GetUtcNow().AddTicks(1);
        var stableFilter = filter with { CreatedBefore = snapshotCreatedBefore };

        var skip = 0;
        const int pageSize = 200;

        while (true)
        {
            var page = await store.GetRunsAsync(stableFilter, skip, pageSize, cancellationToken);
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

    public Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default)
        => store.GetBatchAsync(batchId, cancellationToken);

    public async Task CancelAsync(string runId, CancellationToken cancellationToken = default)
    {
        var result = await runCancellation.CancelRunAndDescendantsAsync(runId, ClientCancellationReason,
            cancellationToken);
        if (!result.Found)
        {
            throw new RunNotFoundException(runId);
        }
    }

    public async Task CancelBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        var result = await runCancellation.CancelBatchSubtreeAsync(batchId, ClientCancellationReason,
            cancellationToken);
        if (!result.Found)
        {
            throw new BatchNotFoundException(batchId);
        }
    }

    public async Task<JobRun> RerunAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        var existingBatch = await store.GetBatchAsync(runId, cancellationToken);
        if (existingBatch is { })
        {
            throw new InvalidOperationException(
                $"'{runId}' is a batch ID. To rerun a batch, retrieve the runs with GetRunsAsync and call TriggerBatchAsync.");
        }

        string? derivedId = null;
        string? derivedDedup = null;
        string? orchestratorRunId = null;
        int durableStep = 0;
        if (JobContext.Current is { OrchestratorRunId: { } orchId } ctx)
        {
            // AllocateStep returns (step, isReplay) so concurrent client calls in a
            // Task.WhenAll fan-out each capture their own replay flag before any await -
            // the shared IsReplaying read would race against peer increments.
            var (step, isReplay) = ctx.AllocateStep();
            orchestratorRunId = orchId;
            durableStep = step;
            derivedId = DurableIds.DerivedRunId(orchId, durableStep);
            derivedDedup = DurableIds.DedupId(orchId, durableStep);
            if (isReplay)
            {
                ctx.ValidateReplayChildRun(durableStep, derivedId);
            }

            var existing = await store.GetRunAsync(derivedId, cancellationToken);
            if (existing is { })
            {
                return existing;
            }
        }

        var requestedPriority = await ResolveRequestedPriorityAsync(run.JobName, null, cancellationToken);
        var rerun = CreateRun(
            run.JobName,
            run.Arguments,
            derivedDedup is { } d ? new() { DeduplicationId = d } : new(),
            timeProvider.GetUtcNow(),
            requestedPriority ?? 0,
            runId: derivedId,
            rerunOfRunId: run.Id,
            isDurable: run.IsDurable);

        var clonedInputEvents = await BuildClonedRunScopedInputEventsAsync(runId, rerun.Id, cancellationToken);

        // Atomically update the orchestrator's highest_recorded_step alongside the rerun insert.
        DurableStepRecord? stepRecord = orchestratorRunId is { } recordingOrchId
            ? new DurableStepRecord(recordingOrchId, durableStep)
            : null;

        if (!await store.TryCreateRunAsync(
                rerun,
                initialEvents: clonedInputEvents,
                durableStepRecord: stepRecord,
                cancellationToken: cancellationToken))
        {
            var existing = await store.GetRunAsync(rerun.Id, cancellationToken);
            if (existing is { })
            {
                return existing;
            }

            throw new RunConflictException(rerun.Id, $"Run '{rerun.Id}' already exists.");
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);
        return rerun;
    }

    public async IAsyncEnumerable<RunEvent> ObserveRunEventsAsync(string runId, long sinceEventId = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (JobContext.Current is { OrchestratorRunId: not null })
        {
            await foreach (var @event in DurableObserveRunEventsAsync(runId, sinceEventId, cancellationToken))
            {
                yield return @event;
            }

            yield break;
        }

        using var wakeup = new SemaphoreSlim(0, 1);
        await using var eventSub = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(runId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);
        await using var completionSub = await notifications.SubscribeAsync(
            NotificationChannels.RunTerminated(runId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        while (true)
        {
            var run = await store.GetRunAsync(runId, cancellationToken);
            if (run is null)
            {
                throw new RunNotFoundException(runId);
            }

            while (true)
            {
                var events = await store.GetEventsAsync(runId, sinceEventId,
                    cancellationToken: cancellationToken);
                if (events.Count == 0)
                {
                    break;
                }

                foreach (var @event in events)
                {
                    sinceEventId = @event.Id;
                    yield return @event;
                }
            }

            if (run.Status.IsTerminal)
            {
                yield break;
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
    }

    private async IAsyncEnumerable<RunEvent> DurableObserveRunEventsAsync(string runId, long sinceEventId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        if (!run.Status.IsTerminal)
        {
            JobContext.Current?.PendingAwaits?.AddRun(runId);
            throw new DurableYieldException();
        }

        while (true)
        {
            var events = await store.GetEventsAsync(runId, sinceEventId,
                cancellationToken: cancellationToken);
            if (events.Count == 0)
            {
                yield break;
            }

            foreach (var @event in events)
            {
                sinceEventId = @event.Id;
                yield return @event;
            }
        }
    }

    public async IAsyncEnumerable<RunEvent> ObserveBatchEventsAsync(string batchId, long sinceEventId = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (JobContext.Current is { OrchestratorRunId: not null })
        {
            await foreach (var @event in DurableObserveBatchEventsAsync(batchId, sinceEventId, cancellationToken))
            {
                yield return @event;
            }

            yield break;
        }

        using var wakeup = new SemaphoreSlim(0, 1);
        await using var batchEventSub = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);
        await using var batchTerminatedSub = await notifications.SubscribeAsync(
            NotificationChannels.BatchTerminated(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        while (true)
        {
            var batch = await store.GetBatchAsync(batchId, cancellationToken);
            if (batch is null)
            {
                throw new BatchNotFoundException(batchId);
            }

            while (true)
            {
                var events = await store.GetBatchEventsAsync(batchId, sinceEventId, BatchEventPageSize, cancellationToken);
                if (events.Count == 0)
                {
                    break;
                }

                foreach (var @event in events)
                {
                    sinceEventId = @event.Id;
                    yield return @event;
                }
            }

            if (batch.Status.IsTerminal)
            {
                yield break;
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
    }

    private async IAsyncEnumerable<RunEvent> DurableObserveBatchEventsAsync(string batchId, long sinceEventId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var batch = await store.GetBatchAsync(batchId, cancellationToken);
        if (batch is null)
        {
            throw new BatchNotFoundException(batchId);
        }

        if (!batch.Status.IsTerminal)
        {
            JobContext.Current?.PendingAwaits?.AddBatch(batchId);
            throw new DurableYieldException();
        }

        while (true)
        {
            var events = await store.GetBatchEventsAsync(batchId, sinceEventId, BatchEventPageSize, cancellationToken);
            if (events.Count == 0)
            {
                yield break;
            }

            foreach (var @event in events)
            {
                sinceEventId = @event.Id;
                yield return @event;
            }
        }
    }

    public async Task<JobRun> WaitAsync(string runId, CancellationToken cancellationToken = default)
    {
        if (JobContext.Current is { OrchestratorRunId: not null })
        {
            return await DurableWaitRunAsync(runId, cancellationToken);
        }

        await foreach (var _ in ObserveRunEventsAsync(runId, 0, cancellationToken))
        {
        }

        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        return run;
    }

    private async Task<JobRun> DurableWaitRunAsync(string runId, CancellationToken cancellationToken)
    {
        // Claim-time snapshot served the orchestrator's recorded children in one bulk read.
        // Most replay paths satisfy themselves from the dictionary; the store fetch is only
        // for runs created outside the orchestrator's scope (external WaitAsync).
        if (JobContext.Current?.DurableSnapshot is { } snap
            && snap.Children.TryGetValue(runId, out var cached)
            && cached.Status.IsTerminal)
        {
            return cached;
        }

        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        if (!run.Status.IsTerminal)
        {
            JobContext.Current?.PendingAwaits?.AddRun(runId);
            throw new DurableYieldException();
        }

        return run;
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public Task<T> RunAsync<T>(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default) =>
        RunAsync<T>(job, AsRunArguments(args), options, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public Task RunAsync(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default) =>
        RunAsync(job, AsRunArguments(args), options, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default) =>
        StreamAsync<T>(job, AsRunArguments(args), options, cancellationToken);

    public async Task<JobBatch> WaitBatchAsync(string batchId, CancellationToken cancellationToken = default)
    {
        if (JobContext.Current is { OrchestratorRunId: not null })
        {
            return await DurableWaitBatchAsync(batchId, cancellationToken);
        }

        using var wakeup = new SemaphoreSlim(0, 1);
        await using var subscription = await notifications.SubscribeAsync(
            NotificationChannels.BatchTerminated(batchId),
            _ => ReleaseWakeupAsync(wakeup),
            cancellationToken);

        while (true)
        {
            var batch = await store.GetBatchAsync(batchId, cancellationToken);
            if (batch is null)
            {
                throw new BatchNotFoundException(batchId);
            }

            if (batch.Status.IsTerminal)
            {
                return batch;
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
    }

    private async Task<JobBatch> DurableWaitBatchAsync(string batchId, CancellationToken cancellationToken)
    {
        // Claim-time snapshot served orchestrator-owned batches in one bulk read.
        if (JobContext.Current?.DurableSnapshot is { } snap
            && snap.ChildBatches.TryGetValue(batchId, out var cached)
            && cached.Status.IsTerminal)
        {
            return cached;
        }

        var batch = await store.GetBatchAsync(batchId, cancellationToken);
        if (batch is null)
        {
            throw new BatchNotFoundException(batchId);
        }

        if (!batch.Status.IsTerminal)
        {
            JobContext.Current?.PendingAwaits?.AddBatch(batchId);
            throw new DurableYieldException();
        }

        return batch;
    }

    public async IAsyncEnumerable<JobRun> WaitEachAsync(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Yield child snapshots as each becomes terminal, regardless of outcome. Bulk-fetches
        // in windows so per-child round trips amortize. Relies on the invariant that
        // batch.Status.IsTerminal is set atomically with the last child's terminal transition,
        // so when ObserveBatchEventsAsync exits, every child's terminal event has been drained.
        var pendingIds = new List<string>(BatchFetchWindowSize);
        DateTimeOffset? windowDeadline = null;

        async Task<IReadOnlyList<JobRun>> FlushAsync()
        {
            if (pendingIds.Count == 0)
            {
                return [];
            }

            var fetched = await store.GetRunsByIdsAsync(pendingIds.ToArray(), cancellationToken);
            pendingIds.Clear();
            windowDeadline = null;
            return fetched;
        }

        await foreach (var @event in ObserveBatchEventsAsync(batchId, 0, cancellationToken))
        {
            if (!RunStatusEvents.IsTerminal(@event))
            {
                continue;
            }

            pendingIds.Add(@event.RunId);
            windowDeadline ??= timeProvider.GetUtcNow() + BatchFetchWindowDelay;

            if (pendingIds.Count >= BatchFetchWindowSize
                || timeProvider.GetUtcNow() >= windowDeadline)
            {
                foreach (var run in await FlushAsync())
                {
                    yield return run;
                }
            }
        }

        foreach (var run in await FlushAsync())
        {
            yield return run;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public async Task<IReadOnlyList<T>> RunBatchAsync<T>(string job, IEnumerable<object?> args,
        BatchRunOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        return await RunBatchAsync<T>(job, args.Select(AsRunArguments), options, cancellationToken);
    }

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public Task RunBatchAsync(string job, IEnumerable<object?> args, BatchRunOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        return RunBatchAsync(job, args.Select(AsRunArguments), options, cancellationToken);
    }

    public async Task RunBatchAsync(string job, IEnumerable<RunArguments?> args, BatchRunOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(job, args, options, cancellationToken);
        try
        {
            await WaitBatchNonGenericAsync(batch.Id, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryCancelOwnedBatchAsync(batch.Id);
            throw;
        }
    }

    public async Task<IReadOnlyList<T>> RunBatchAsync<T>(IEnumerable<BatchItem> items,
        CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(items, cancellationToken);
        try
        {
            return await WaitBatchAsync<T>(batch.Id, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryCancelOwnedBatchAsync(batch.Id);
            throw;
        }
    }

    public async Task RunBatchAsync(IEnumerable<BatchItem> items, CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(items, cancellationToken);
        try
        {
            await WaitBatchNonGenericAsync(batch.Id, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryCancelOwnedBatchAsync(batch.Id);
            throw;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> StreamBatchAsync<T>(string job, IEnumerable<object?> args,
        BatchRunOptions? options = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(args);
        return StreamBatchAsync<T>(job, args.Select(AsRunArguments), options, cancellationToken);
    }

    public async IAsyncEnumerable<T> StreamBatchAsync<T>(IEnumerable<BatchItem> items,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(items, cancellationToken);
        await foreach (var item in OwnedStreamBatchAsync<T>(batch.Id, cancellationToken))
        {
            yield return item;
        }
    }

    private RunArguments? AsRunArguments(object? args) => args switch
    {
        null => null,
        RunArguments ra => ra,
        _ => SerializeRuntimeArgs(args)
    };

    private RunArguments SerializeRuntimeArgs(object args)
    {
        var typeInfo = _serializerOptions.GetTypeInfo(args.GetType());
        return new() { Json = JsonSerializer.Serialize(args, typeInfo) };
    }

    private async IAsyncEnumerable<T> OwnedStreamBatchAsync<T>(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var enumerator = WaitEachAsync<T>(batchId, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        var completedOrYielded = false;
        try
        {
            while (true)
            {
                bool hasNext;
                try
                {
                    hasNext = await enumerator.MoveNextAsync();
                }
                catch (DurableYieldException)
                {
                    // Durable yield is a suspend signal from the executor, not consumer
                    // abandonment. Don't cancel the batch we just created - the orchestrator will
                    // resume and re-enter with the recorded children terminal.
                    completedOrYielded = true;
                    throw;
                }

                if (!hasNext)
                {
                    completedOrYielded = true;
                    yield break;
                }

                yield return enumerator.Current;
            }
        }
        finally
        {
            if (!completedOrYielded)
            {
                await TryCancelOwnedBatchAsync(batchId);
            }
        }
    }

    private async Task WaitBatchNonGenericAsync(string batchId, CancellationToken cancellationToken)
    {
        var failures = new List<Exception>();
        await foreach (var child in WaitEachAsync(batchId, cancellationToken))
        {
            if (child.Status is JobStatus.Failed or JobStatus.Canceled)
            {
                failures.Add(await BuildJobRunExceptionAsync(child, cancellationToken));
            }
        }

        if (failures.Count > 0)
        {
            throw new AggregateException(failures);
        }
    }

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    internal Task<string> TriggerAllAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default) =>
        TriggerAllAsync(jobName, argsList, new(), cancellationToken);

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    internal async Task<string> TriggerAllAsync(string jobName, IEnumerable<object?> argsList,
        BatchRunOptions options, CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(jobName, argsList, options, cancellationToken);
        return batch.Id;
    }

    private async Task<JobBatch> TriggerBatchHeterogeneousAsync(IEnumerable<BatchItem> items,
        CancellationToken cancellationToken)
    {
        var itemsList = items as IReadOnlyList<BatchItem> ?? items.ToList();
        var normalized = new List<(string JobName, RunArguments Args, BatchRunOptions? Options)>(itemsList.Count);
        var priorityByJob = new Dictionary<string, int?>(StringComparer.Ordinal);

        // One GetJobAsync per distinct jobName; homogeneous batches collapse to a single lookup.
        var durableByJob = new Dictionary<string, bool>(StringComparer.Ordinal);
        foreach (var item in itemsList)
        {
            if (!priorityByJob.ContainsKey(item.JobName))
            {
                var meta = await LookupJobMetadataAsync(item.JobName, cancellationToken);
                priorityByJob[item.JobName] = meta.Priority;
                durableByJob[item.JobName] = meta.IsDurable;
            }

            normalized.Add((item.JobName, item.Args ?? RunArguments.Empty, item.Options));
        }

        return await TriggerBatchCoreAsync(normalized, priorityByJob, durableByJob, cancellationToken);
    }

    private async Task<int?> LookupJobPriorityAsync(string jobName, CancellationToken cancellationToken)
    {
        var priority = (await store.GetJobAsync(jobName, cancellationToken))?.Priority;
        if (priority is null)
        {
            Log.TriggerRequestedForUnknownJob(logger, jobName);
        }

        return priority;
    }

    private async Task<(int? Priority, bool IsDurable)> LookupJobMetadataAsync(string jobName,
        CancellationToken cancellationToken)
    {
        var job = await store.GetJobAsync(jobName, cancellationToken);
        if (job is null)
        {
            Log.TriggerRequestedForUnknownJob(logger, jobName);
        }

        return (job?.Priority, job?.IsDurable ?? false);
    }

    private async Task<JobBatch> TriggerBatchCoreAsync(
        IReadOnlyList<(string JobName, RunArguments Args, BatchRunOptions? Options)> items,
        IReadOnlyDictionary<string, int?> priorityByJob,
        IReadOnlyDictionary<string, bool> durableByJob,
        CancellationToken cancellationToken)
    {
        var now = timeProvider.GetUtcNow();
        string batchId;
        string? orchestratorRunId = null;
        int durableStep = 0;
        if (JobContext.Current is { OrchestratorRunId: { } orchId } ctx)
        {
            var (step, isReplay) = ctx.AllocateStep();
            orchestratorRunId = orchId;
            durableStep = step;
            batchId = DurableIds.DerivedBatchId(orchId, durableStep);
            if (isReplay)
            {
                ctx.ValidateReplayChildBatch(durableStep, batchId);
            }

            // Claim-time snapshot serves the existence check from memory during replay.
            var existing = ctx.DurableSnapshot is { } snap
                           && snap.ChildBatches.TryGetValue(batchId, out var snapBatch)
                ? snapBatch
                : await store.GetBatchAsync(batchId, cancellationToken);
            if (existing is { })
            {
                // Crash-and-replay: if any child's input pump never wrote InputComplete for one
                // or more declared streams, advance the source past the recorded sequence and
                // restart the pump. Without this, the child awaits an InputComplete that nobody
                // will ever write.
                await ResumeBatchInputPumpsIfNeededAsync(existing.Id, items, cancellationToken);
                return existing;
            }
        }
        else
        {
            batchId = CreateRunId();
        }

        var batch = new JobBatch
        {
            Id = batchId,
            Status = JobStatus.Pending,
            Total = items.Count,
            CreatedAt = now,
            ParentRunId = orchestratorRunId ?? JobContext.Current?.RunId
        };

        var runs = new List<JobRun>(items.Count);
        var streamPumps = new List<(string RunId, IReadOnlyList<RunArgumentStream> Streams)>();
        var initialEvents = new List<RunEvent>();

        for (var index = 0; index < items.Count; index++)
        {
            var (jobName, args, itemOptions) = items[index];
            var requestedPriority = itemOptions?.Priority ?? priorityByJob[jobName];
            var argumentsJson = MaterializeJson(args);
            string? childId = null;
            if (orchestratorRunId is not null)
            {
                childId = DurableIds.DerivedBatchChildRunId(batchId, index);
            }

            var child = CreateRun(jobName, argumentsJson, FromBatch(itemOptions), now, requestedPriority ?? 0,
                runId: childId,
                isDurable: durableByJob.TryGetValue(jobName, out var dur) && dur);
            child = child with { BatchId = batchId, RootRunId = child.RootRunId ?? batchId };
            runs.Add(child);

            if (args.Streams.Count > 0)
            {
                streamPumps.Add((child.Id, args.Streams));
                initialEvents.AddRange(BuildInitialEventsFromAot(child.Id, args.Streams));
            }
        }

        // Atomically update the orchestrator's highest_recorded_step alongside the batch insert.
        DurableStepRecord? batchStepRecord = orchestratorRunId is { } batchRecordingOrchId
            ? new DurableStepRecord(batchRecordingOrchId, durableStep)
            : null;

        try
        {
            await store.CreateBatchAsync(batch, runs, initialEvents,
                durableStepRecord: batchStepRecord, cancellationToken: cancellationToken);
        }
        catch (RunConflictException) when (orchestratorRunId is not null)
        {
            // Durable replay re-derived the same batch id. Surface the existing batch instead of
            // failing the replay. Narrow to RunConflictException so genuine errors (transient
            // store failures, cancellation, serialization bugs) still bubble up.
            var existing = await store.GetBatchAsync(batchId, cancellationToken);
            if (existing is { })
            {
                return existing;
            }

            throw;
        }

        if (items.Count == 0)
        {
            var completedAt = timeProvider.GetUtcNow();
            if (await store.TryCompleteBatchAsync(batchId, JobStatus.Succeeded, completedAt, cancellationToken))
            {
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId,
                    cancellationToken);
            }
        }
        else
        {
            await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);
            foreach (var (runId, streams) in streamPumps)
            {
                StartAotInputPump(runId, streams);
            }
        }

        return batch;
    }

    private static RunOptions FromBatch(BatchRunOptions? batch) =>
        batch is null
            ? new()
            : new()
            {
                NotBefore = batch.NotBefore,
                NotAfter = batch.NotAfter,
                ExpiresAt = batch.ExpiresAt,
                Priority = batch.Priority
            };

    private async Task<bool> HasOutputCompleteForAttemptAsync(string runId, int attempt,
        CancellationToken cancellationToken)
    {
        var events = await store.GetEventsAsync(runId, 0, [RunEventType.OutputComplete],
            attempt, 1, cancellationToken);
        return events.Count > 0;
    }

    private async Task<IReadOnlyList<string>> ReadAttemptOutputPayloadsAsync(string runId, int attempt,
        CancellationToken cancellationToken)
    {
        var events = await store.GetEventsAsync(runId, 0, [RunEventType.Output],
            attempt, cancellationToken: cancellationToken);
        if (events.Count == 0)
        {
            return [];
        }

        var payloads = new List<string>(events.Count);
        foreach (var @event in events)
        {
            payloads.Add(@event.Payload);
        }

        return payloads;
    }


    private async Task ThrowIfNonSuccessTerminalAsync(JobRun run, CancellationToken cancellationToken)
    {
        if (run.Status is JobStatus.Failed or JobStatus.Canceled)
        {
            throw await BuildJobRunExceptionAsync(run, cancellationToken);
        }
    }

    /// <summary>
    ///     Builds a <see cref="JobRunException" /> with a meaningful message. Prefers
    ///     <see cref="JobRun.Reason" /> for non-exception terminations (cancel, expiration,
    ///     no-handler, shutdown). For retry-exhaustion Failed runs Reason is null, so we
    ///     read the last <c>AttemptFailure</c> event for the final attempt's detail.
    /// </summary>
    private async Task<JobRunException> BuildJobRunExceptionAsync(JobRun run, CancellationToken cancellationToken)
    {
        if (run.Reason is { Length: > 0 })
        {
            return new(run.Id, run.Status, run.Reason);
        }

        if (run.Status == JobStatus.Failed)
        {
            var failures = await store.GetEventsAsync(
                run.Id, 0, [RunEventType.AttemptFailure],
                run.Attempt, cancellationToken: cancellationToken);

            if (failures.Count > 0)
            {
                var detail = TryExtractFailureMessage(failures[^1].Payload);
                if (detail is { })
                {
                    return new(run.Id, run.Status, detail);
                }
            }
        }

        return new(run.Id, run.Status, null);
    }

    private static string? TryExtractFailureMessage(string? payload)
    {
        if (string.IsNullOrEmpty(payload))
        {
            return null;
        }

        RunFailureEnvelope? envelope;
        try
        {
            envelope = JsonSerializer.Deserialize(payload, SurefireJsonContext.Default.RunFailureEnvelope);
        }
        catch (JsonException)
        {
            return null;
        }

        if (envelope is null)
        {
            return null;
        }

        return (envelope.ExceptionType, envelope.Message) switch
        {
            ({ Length: > 0 } type, { Length: > 0 } message) => $"{type}: {message}",
            (_, { Length: > 0 } message) => message,
            ({ Length: > 0 } type, _) => type,
            _ => null
        };
    }

    private JobRun CreateRun(string jobName, string? serializedArguments, RunOptions runOptions,
        DateTimeOffset now, int priority, string? runId = null, string? rerunOfRunId = null,
        bool isDurable = false)
    {
        var notBefore = runOptions.NotBefore ?? now;
        var expiresAt = runOptions.ExpiresAt
                        ?? JobRunDefaults.GetDefaultExpiresAt(notBefore, options);

        var run = new JobRun
        {
            Id = runId ?? CreateRunId(),
            JobName = jobName,
            Status = JobStatus.Pending,
            Arguments = serializedArguments,
            CreatedAt = now,
            NotBefore = notBefore,
            NotAfter = runOptions.NotAfter,
            ExpiresAt = expiresAt,
            Priority = priority,
            DeduplicationId = runOptions.DeduplicationId,
            RerunOfRunId = rerunOfRunId,
            Progress = 0,
            Attempt = 1,
            IsDurable = isDurable,
            ParentTraceId = Activity.Current?.TraceId.ToString(),
            ParentSpanId = Activity.Current?.SpanId.ToString()
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

    private async Task<(int? Priority, bool IsDurable)> ResolveTriggerMetadataAsync(string jobName,
        int? explicitPriority, CancellationToken cancellationToken)
    {
        var job = await store.GetJobAsync(jobName, cancellationToken);
        if (job is null)
        {
            Log.TriggerRequestedForUnknownJob(logger, jobName);
        }

        return (explicitPriority ?? job?.Priority, job?.IsDurable ?? false);
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


    private async Task<bool> AppendInputEventAsync(string runId, RunEventType eventType, InputEnvelope payload,
        CancellationToken cancellationToken)
    {
        return await eventWriter.EnqueueIfRunNonTerminalAsync(
            new()
            {
                RunId = runId,
                EventType = eventType,
                Payload = JsonSerializer.Serialize(payload, SurefireJsonContext.Default.InputEnvelope),
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = 0
            },
            [new(NotificationChannels.RunInput(runId), runId)],
            cancellationToken);
    }

    // Fresh CTS bounded by ShutdownTimeout: a caller's already-canceled token must not
    // short-circuit cleanup, but cleanup must not block host shutdown indefinitely either.
    private async Task TryCancelOwnedRunAsync(string runId)
    {
        using var cts = new CancellationTokenSource(options.ShutdownTimeout);
        try
        {
            await runCancellation.CancelRunAndDescendantsAsync(runId, OwnedOperationCancellationReason, cts.Token);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not AccessViolationException)
        {
            Log.FailedToPropagateCancellation(logger, ex, runId);
        }
    }

    private async Task TryCancelOwnedBatchAsync(string batchId)
    {
        using var cts = new CancellationTokenSource(options.ShutdownTimeout);
        try
        {
            await runCancellation.CancelBatchSubtreeAsync(batchId, OwnedOperationCancellationReason, cts.Token);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not AccessViolationException)
        {
            Log.FailedToPropagateBatchCancellation(logger, ex, batchId);
        }
    }

    private static Task ReleaseWakeupAsync(SemaphoreSlim wakeup) => WakeupSignal.ReleaseAsync(wakeup);

    private Task WaitForWakeupAsync(SemaphoreSlim wakeup, CancellationToken cancellationToken) =>
        WakeupSignal.WaitAsync(wakeup, options.PollingInterval, cancellationToken);

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

        [LoggerMessage(EventId = 1015, Level = LogLevel.Warning,
            Message = "Failed to propagate cancellation for batch '{BatchId}'.")]
        public static partial void FailedToPropagateBatchCancellation(ILogger logger, Exception exception,
            string batchId);

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
}
