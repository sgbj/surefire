using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using System.Threading.Channels;
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
    private const int CancellationTraversalPageSize = 100;
    private const string ClientCancellationReason = "Canceled by client request.";
    private const string OwnedOperationCancellationReason = "Canceled because the owning operation was canceled.";
    private static readonly TimeSpan BatchFetchWindowDelay = TimeSpan.FromMilliseconds(10);

    private readonly ConcurrentDictionary<string, CancellationTokenSource> _inputPumpTokens =
        new(StringComparer.Ordinal);

    private readonly JsonSerializerOptions _serializerOptions = options.SerializerOptions;

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public async Task<JobRun> TriggerAsync(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var runOptions = options ?? new();
        var prepared = PrepareArguments(args);
        var requestedPriority = await ResolveRequestedPriorityAsync(job, runOptions.Priority, cancellationToken);
        var run = CreateRun(job, prepared.SerializedArguments, runOptions, timeProvider.GetUtcNow(),
            requestedPriority ?? 0);
        var initialEvents = BuildInitialEvents(run.Id, prepared.StreamDeclaration);
        var created = await store.TryCreateRunAsync(run, initialEvents: initialEvents,
            cancellationToken: cancellationToken);
        if (!created)
        {
            throw new RunConflictException(run.Id, $"Run creation for job '{job}' was rejected.");
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);
        if (prepared.Streams.Count > 0)
        {
            StartInputPump(run.Id, prepared.Streams);
        }

        return run;
    }

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public Task<JobBatch> TriggerBatchAsync(IEnumerable<BatchItem> runs, CancellationToken cancellationToken = default)
        => TriggerBatchAsyncCore(runs, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public Task<JobBatch> TriggerBatchAsync(string job, IEnumerable<object?> args, BatchRunOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var items = args.Select(a => new BatchItem(job, a, options));
        return TriggerBatchAsyncCore(items, cancellationToken);
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
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is null)
        {
            throw new RunNotFoundException(runId);
        }

        await runCancellation.CancelRunAndDescendantsAsync(runId, ClientCancellationReason, cancellationToken);
    }

    public async Task CancelBatchAsync(string batchId, CancellationToken cancellationToken = default)
        => await CancelBatchAsync(batchId, ClientCancellationReason, cancellationToken);

    private async Task CancelBatchAsync(string batchId, string reason, CancellationToken cancellationToken)
    {
        var CanceledRunIds = await store.CancelBatchRunsAsync(batchId, reason, cancellationToken);
        foreach (var runId in CanceledRunIds.Distinct(StringComparer.Ordinal))
        {
            await runCancellation.PublishCancellationNotificationsAsync(runId, cancellationToken);
            await notifications.PublishAsync(NotificationChannels.RunEvent(batchId), runId, cancellationToken);
        }

        await CancelBatchRunDescendantsAsync(batchId, reason, new HashSet<string>(StringComparer.Ordinal),
            cancellationToken);

        if (CanceledRunIds.Count == 0)
        {
            return;
        }

        var batch = await store.GetBatchAsync(batchId, cancellationToken);
        if (batch is null)
        {
            return;
        }

        var batchStatus = batch.Failed > 0 ? JobStatus.Failed : JobStatus.Canceled;
        var completedAt = timeProvider.GetUtcNow();
        if (await store.TryCompleteBatchAsync(batchId, batchStatus, completedAt, cancellationToken))
        {
            await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId,
                cancellationToken);
        }
    }

    private async Task CancelBatchRunDescendantsAsync(string batchId, string reason, ISet<string> visited,
        CancellationToken cancellationToken)
    {
        var skip = 0;
        while (true)
        {
            var page = await store.GetRunsAsync(
                new() { BatchId = batchId },
                skip,
                CancellationTraversalPageSize,
                cancellationToken);
            if (page.Items.Count == 0)
            {
                return;
            }

            foreach (var run in page.Items)
            {
                await runCancellation.CancelRunAndDescendantsAsync(
                    run.Id,
                    reason,
                    visited,
                    cancellationToken,
                    cancelSelf: false);
            }

            skip += page.Items.Count;
            if (skip >= page.TotalCount)
            {
                return;
            }
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

        await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);
        return rerun;
    }

    public async IAsyncEnumerable<RunEvent> ObserveRunEventsAsync(string runId, long sinceEventId = 0,
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

    public async IAsyncEnumerable<RunEvent> ObserveBatchEventsAsync(string batchId, long sinceEventId = 0,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
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
                throw new InvalidOperationException($"Batch '{batchId}' was not found.");
            }

            while (true)
            {
                var events = await store.GetBatchEventsAsync(batchId, sinceEventId, 200, cancellationToken);
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

    public async Task<JobRun> WaitAsync(string runId, CancellationToken cancellationToken = default)
    {
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

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public async Task<T> WaitAsync<T>(string runId, CancellationToken cancellationToken = default)
    {
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            var elementType = typeof(T).GetGenericArguments()[0];
            var buildMethod = typeof(JobClient)
                .GetMethod(nameof(BuildHydratedLiveStream), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(elementType);
            return (T)buildMethod.Invoke(this, [runId, cancellationToken])!;
        }

        var run = await WaitAsync(runId, cancellationToken);
        return await HydrateRunAsync<T>(run, cancellationToken);
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, CancellationToken cancellationToken = default)
        => StreamRunHydratedAsync<T>(runId, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public async Task<T> RunAsync<T>(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default)
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

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public async Task RunAsync(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var run = await TriggerAsync(job, args, options, cancellationToken);
        try
        {
            var final = await WaitAsync(run.Id, cancellationToken);
            await ThrowIfNonSuccessTerminalAsync(final, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            await TryCancelOwnedRunAsync(run.Id);
            throw;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public async IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var run = await TriggerAsync(job, args, options, cancellationToken);
        await foreach (var item in OwnedRunStreamAsync<T>(run.Id, cancellationToken))
        {
            yield return item;
        }
    }

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
            {
                throw new InvalidOperationException($"Batch '{batchId}' was not found.");
            }

            if (batch.Status.IsTerminal)
            {
                return batch;
            }

            await WaitForWakeupAsync(wakeup, cancellationToken);
        }
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
    public async Task<IReadOnlyList<T>> WaitBatchAsync<T>(string batchId,
        CancellationToken cancellationToken = default)
    {
        var results = new List<T>();
        var failures = new List<Exception>();

        await foreach (var item in StreamBatchHydratedAsync<T>(batchId, false,
                           cancellationToken))
        {
            if (item.Exception is { } ex)
            {
                failures.Add(ex);
                continue;
            }

            results.Add(item.Value!);
        }

        if (failures.Count > 0)
        {
            throw new AggregateException(failures);
        }

        return results;
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public IAsyncEnumerable<T> WaitEachAsync<T>(string batchId,
        CancellationToken cancellationToken = default)
        => StreamBatchHydratedValuesAsync<T>(batchId, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public async Task<IReadOnlyList<T>> RunBatchAsync<T>(string job, IEnumerable<object?> args,
        BatchRunOptions? options = null, CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(job, args, options, cancellationToken);
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

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    public async Task RunBatchAsync(string job, IEnumerable<object?> args, BatchRunOptions? options = null,
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

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
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

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
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
    public async IAsyncEnumerable<T> StreamBatchAsync<T>(string job, IEnumerable<object?> args,
        BatchRunOptions? options = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(job, args, options, cancellationToken);
        await foreach (var item in OwnedStreamBatchAsync<T>(batch.Id, cancellationToken))
        {
            yield return item;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    public async IAsyncEnumerable<T> StreamBatchAsync<T>(IEnumerable<BatchItem> items,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(items, cancellationToken);
        await foreach (var item in OwnedStreamBatchAsync<T>(batch.Id, cancellationToken))
        {
            yield return item;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async IAsyncEnumerable<T> OwnedStreamBatchAsync<T>(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var enumerator = WaitEachAsync<T>(batchId, cancellationToken)
            .GetAsyncEnumerator(cancellationToken);
        var completed = false;
        try
        {
            while (true)
            {
                T item;
                if (!await enumerator.MoveNextAsync())
                {
                    completed = true;
                    yield break;
                }

                item = enumerator.Current;

                yield return item;
            }
        }
        finally
        {
            if (!completed)
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
        var batch = await TriggerBatchAsyncCore(argsList.Select(a => new BatchItem(jobName, a, options)),
            cancellationToken);
        return batch.Id;
    }

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    private async Task<JobBatch> TriggerBatchAsyncCore(IEnumerable<BatchItem> items,
        CancellationToken cancellationToken = default)
    {
        var itemsList = items as IReadOnlyList<BatchItem> ?? items.ToList();
        var now = timeProvider.GetUtcNow();
        var batchId = CreateRunId();
        var batch = new JobBatch
        {
            Id = batchId, Status = JobStatus.Pending, Total = itemsList.Count, CreatedAt = now
        };

        // One GetJobAsync per distinct jobName; homogeneous batches collapse to a single lookup.
        var priorityByJob = new Dictionary<string, int?>(StringComparer.Ordinal);

        var runs = new List<JobRun>(itemsList.Count);
        var streamPumps = new List<(string RunId, IReadOnlyList<StreamingArgumentSource> Streams,
            InputDeclarationEnvelope Declaration)>();
        foreach (var item in itemsList)
        {
            if (!priorityByJob.TryGetValue(item.JobName, out var jobDefaultPriority))
            {
                jobDefaultPriority = (await store.GetJobAsync(item.JobName, cancellationToken))?.Priority;
                if (jobDefaultPriority is null)
                {
                    Log.TriggerRequestedForUnknownJob(logger, item.JobName);
                }

                priorityByJob[item.JobName] = jobDefaultPriority;
            }

            var requestedPriority = item.Options?.Priority ?? jobDefaultPriority;
            var prepared = PrepareArguments(item.Args);
            var runOptions = item.Options is { } batchOpts
                ? new()
                {
                    NotBefore = batchOpts.NotBefore,
                    NotAfter = batchOpts.NotAfter,
                    Priority = batchOpts.Priority
                }
                : new RunOptions();
            var child = CreateRun(item.JobName, prepared.SerializedArguments, runOptions, now,
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
            {
                await notifications.PublishAsync(NotificationChannels.BatchTerminated(batchId), batchId,
                    cancellationToken);
            }
        }
        else
        {
            // One RunCreated publish per batch (edge-trigger wakeup), not per child.
            await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);

            foreach (var (runId, streams, _) in streamPumps)
            {
                StartInputPump(runId, streams);
            }
        }

        return batch;
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async Task<T> HydrateRunAsync<T>(JobRun run, CancellationToken cancellationToken)
    {
        await ThrowIfNonSuccessTerminalAsync(run, cancellationToken);

        var target = typeof(T);

        if (target.IsGenericType && target.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            // Live stream over a terminal run: yield from run.Result or Output events of the
            // winning attempt.
            var elementType = target.GetGenericArguments()[0];
            var buildMethod = typeof(JobClient)
                .GetMethod(nameof(BuildHydratedLiveStream), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(elementType);
            return (T)buildMethod.Invoke(this, [run.Id, cancellationToken])!;
        }

        if (TypeHelpers.TryGetCollectionElementType(target, out var itemType, out var asArray))
        {
            var method = typeof(JobClient)
                .GetMethod(nameof(HydrateCollectionAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(itemType);
            var task = (Task<object?>)method.Invoke(this, [run, asArray, cancellationToken])!;
            return (T)(await task)!;
        }

        return await HydrateScalarAsync<T>(run, cancellationToken);
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async Task<T> HydrateScalarAsync<T>(JobRun run, CancellationToken cancellationToken)
    {
        if (run.Result is { } resultJson)
        {
            return JsonSerializer.Deserialize<T>(resultJson, _serializerOptions)!;
        }

        // Scalar from Output events: only valid when the winning attempt produced exactly one item.
        if (await HasOutputCompleteForAttemptAsync(run.Id, run.Attempt, cancellationToken))
        {
            var items = await ReadAttemptOutputPayloadsAsync(run.Id, run.Attempt, cancellationToken);
            return items.Count switch
            {
                0 => throw new InvalidOperationException($"Run '{run.Id}' produced no result."),
                1 => JsonSerializer.Deserialize<T>(items[0], _serializerOptions)!,
                _ => throw new InvalidOperationException(
                    $"Run '{run.Id}' produced {items.Count} items; cannot materialize as scalar.")
            };
        }

        throw new InvalidOperationException($"Run '{run.Id}' produced no result.");
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async Task<object?> HydrateCollectionAsync<TElement>(JobRun run, bool asArray,
        CancellationToken cancellationToken)
    {
        List<TElement> items;

        if (run.Result is { } resultJson)
        {
            // result column carries a JSON array, or a single value the serializer handles.
            items = JsonSerializer.Deserialize<List<TElement>>(resultJson, _serializerOptions) ?? [];
        }
        else if (await HasOutputCompleteForAttemptAsync(run.Id, run.Attempt, cancellationToken))
        {
            var payloads = await ReadAttemptOutputPayloadsAsync(run.Id, run.Attempt, cancellationToken);
            items = new(payloads.Count);
            foreach (var payload in payloads)
            {
                items.Add(JsonSerializer.Deserialize<TElement>(payload, _serializerOptions)!);
            }
        }
        else
        {
            throw new InvalidOperationException($"Run '{run.Id}' produced no result.");
        }

        return asArray ? items.ToArray() : items;
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private IAsyncEnumerable<TElement> BuildHydratedLiveStream<TElement>(string runId,
        CancellationToken cancellationToken)
        => StreamRunHydratedAsync<TElement>(runId, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async IAsyncEnumerable<T> StreamRunHydratedAsync<T>(string runId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Retry-transparent: yields every Output event across all attempts in commit order.
        // Throws only if the run reaches a non-success terminal. If the run is already terminal
        // with a result but no Output events, materializes from run.Result.
        long sinceEventId = 0;
        var yieldedAny = false;

        await foreach (var @event in ObserveRunEventsAsync(runId, sinceEventId, cancellationToken))
        {
            sinceEventId = @event.Id;

            if (@event.EventType == RunEventType.Output)
            {
                yieldedAny = true;
                if (TryDeserializeJson(@event.Payload, out T? item, runId, @event.Id, @event.EventType,
                        "run output stream"))
                {
                    yield return item!;
                }
            }
        }

        var run = await store.GetRunAsync(runId, cancellationToken)
                  ?? throw new RunNotFoundException(runId);

        if (run.Status is JobStatus.Failed or JobStatus.Canceled)
        {
            throw await BuildJobRunExceptionAsync(run, cancellationToken);
        }

        // Succeeded with no Output events but a result value: decode as a collection. Scalars
        // yield as a single item.
        if (!yieldedAny && run.Result is { } resultJson)
        {
            var items = TryDeserializeResultAsList<T>(resultJson);
            if (items is { })
            {
                foreach (var item in items)
                {
                    yield return item;
                }

                yield break;
            }

            yield return JsonSerializer.Deserialize<T>(resultJson, _serializerOptions)!;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private List<T>? TryDeserializeResultAsList<T>(string resultJson)
    {
        try
        {
            return JsonSerializer.Deserialize<List<T>>(resultJson, _serializerOptions);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async IAsyncEnumerable<T> StreamBatchHydratedValuesAsync<T>(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var item in StreamBatchHydratedAsync<T>(batchId, true, cancellationToken))
        {
            yield return item.Value!;
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private IAsyncEnumerable<HydratedChild<T>> StreamBatchHydratedAsync<T>(string batchId,
        bool throwOnChildFailure, CancellationToken cancellationToken)
    {
        // IAsyncEnumerable<U>: each child yielded as a live stream. Scalar / collection: batch
        // hydration with per-child Output buffering + bulk terminal fetch.
        if (typeof(T).IsGenericType && typeof(T).GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            // For IAsyncEnumerable<U> batches, per-child failures surface on the child's inner
            // stream, never on the outer (handles are yielded as soon as the child is observed),
            // so throwOnChildFailure is irrelevant here.
            var elementType = typeof(T).GetGenericArguments()[0];
            var method = typeof(JobClient)
                .GetMethod(nameof(StreamBatchLiveInnerAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(elementType);
            return (IAsyncEnumerable<HydratedChild<T>>)method.Invoke(this,
                [batchId, cancellationToken])!;
        }

        return StreamBatchMaterializedAsync<T>(batchId, throwOnChildFailure, cancellationToken);
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async IAsyncEnumerable<HydratedChild<T>> StreamBatchMaterializedAsync<T>(string batchId,
        bool throwOnChildFailure, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // Per-child buffers accumulate Output events for the current attempt. On terminal,
        // hydrate from the buffered events or run.Result via windowed bulk fetch.
        var childBuffers = new Dictionary<string, ChildOutputBuffer>(StringComparer.Ordinal);
        var pendingTerminalIds = new List<string>(BatchFetchWindowSize);
        DateTimeOffset? windowDeadline = null;

        async IAsyncEnumerable<HydratedChild<T>> FlushWindowAsync()
        {
            if (pendingTerminalIds.Count == 0)
            {
                yield break;
            }

            var fetched = await store.GetRunsByIdsAsync(pendingTerminalIds.ToArray(), cancellationToken);
            var byId = fetched.ToDictionary(r => r.Id, StringComparer.Ordinal);
            foreach (var id in pendingTerminalIds)
            {
                if (!byId.TryGetValue(id, out var run))
                {
                    continue;
                }

                var buffer = childBuffers.TryGetValue(id, out var b) ? b : null;
                childBuffers.Remove(id);

                HydratedChild<T> hydrated;
                try
                {
                    var value = await HydrateBatchChildAsync<T>(run, buffer, cancellationToken);
                    hydrated = new(value, null);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // Caller's CT: propagate so the outer enumerator terminates.
                    throw;
                }
                catch (Exception ex)
                {
                    // Per-child hydration failure (JobRunException, no-result, JSON, ...). Collected
                    // into the HydratedChild so WaitBatchAsync<T> aggregates and StreamBatchAsync<T>
                    // fail-fasts on first.
                    hydrated = new(default, ex);
                }

                if (hydrated.Exception is { } ex2 && throwOnChildFailure)
                {
                    throw ex2;
                }

                yield return hydrated;
            }

            pendingTerminalIds.Clear();
            windowDeadline = null;
        }

        // Multiplex ObserveBatchEventsAsync with a deadline timer so windows flush under sparse
        // arrivals. The local function wrap is so a still-pending MoveNextAsync gets observed
        // before disposal; otherwise a child-failure throw would race teardown and surface as
        // "System.NotSupportedException: Specified method is not supported." instead of the real
        // exception.
        await foreach (var hydrated in StreamBatchMaterializedAsyncCore())
        {
            yield return hydrated;
        }

        async IAsyncEnumerable<HydratedChild<T>> StreamBatchMaterializedAsyncCore()
        {
            // Linked CTS we can cancel independently of the caller's token, so an exception
            // can stop the pending MoveNextAsync without waiting out the polling interval.
            using var enumeratorCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            await using var enumerator = ObserveBatchEventsAsync(batchId, 0, enumeratorCts.Token)
                .GetAsyncEnumerator(enumeratorCts.Token);
            var moveTask = enumerator.MoveNextAsync().AsTask();
            try
            {
                while (true)
                {
                    if (windowDeadline is { } dl)
                    {
                        var remaining = dl - timeProvider.GetUtcNow();
                        if (remaining <= TimeSpan.Zero)
                        {
                            await foreach (var hydrated in FlushWindowAsync())
                            {
                                yield return hydrated;
                            }

                            continue;
                        }

                        using var delayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                        var delayTask = Task.Delay(remaining, timeProvider, delayCts.Token);
                        var winner = await Task.WhenAny(moveTask, delayTask);
                        delayCts.Cancel();
                        if (winner != moveTask)
                        {
                            // Deadline fired: flush queued results without blocking on the next event.
                            await foreach (var hydrated in FlushWindowAsync())
                            {
                                yield return hydrated;
                            }

                            continue;
                        }
                    }

                    bool moved;
                    Exception? moveFault = null;
                    try
                    {
                        moved = await moveTask;
                        moveTask = null;
                    }
                    catch (Exception ex)
                    {
                        moved = false;
                        moveFault = ex;
                        moveTask = null;
                    }

                    if (moveFault is { })
                    {
                        await foreach (var hydrated in FlushWindowAsync())
                        {
                            yield return hydrated;
                        }

                        ExceptionDispatchInfo.Capture(moveFault).Throw();
                    }

                    if (!moved)
                    {
                        break;
                    }

                    var @event = enumerator.Current;
                    moveTask = enumerator.MoveNextAsync().AsTask();

                    if (@event.EventType == RunEventType.Output)
                    {
                        if (!childBuffers.TryGetValue(@event.RunId, out var buffer))
                        {
                            buffer = new() { Attempt = @event.Attempt };
                            childBuffers[@event.RunId] = buffer;
                        }

                        // Retry advanced the attempt; reset the buffer to track the winning attempt.
                        if (@event.Attempt > buffer.Attempt)
                        {
                            buffer.Attempt = @event.Attempt;
                            buffer.Payloads.Clear();
                            buffer.OutputCompleted = false;
                        }

                        if (@event.Attempt == buffer.Attempt)
                        {
                            buffer.Payloads.Add(@event.Payload);
                        }

                        continue;
                    }

                    if (@event.EventType == RunEventType.OutputComplete)
                    {
                        // Covers zero-item streams (OutputComplete with no preceding Output events).
                        if (!childBuffers.TryGetValue(@event.RunId, out var buffer))
                        {
                            buffer = new() { Attempt = @event.Attempt };
                            childBuffers[@event.RunId] = buffer;
                        }

                        if (@event.Attempt >= buffer.Attempt)
                        {
                            buffer.Attempt = @event.Attempt;
                            buffer.OutputCompleted = true;
                        }

                        continue;
                    }

                    if (!RunStatusEvents.IsTerminal(@event))
                    {
                        continue;
                    }

                    pendingTerminalIds.Add(@event.RunId);
                    windowDeadline ??= timeProvider.GetUtcNow() + BatchFetchWindowDelay;

                    if (pendingTerminalIds.Count >= BatchFetchWindowSize
                        || timeProvider.GetUtcNow() >= windowDeadline)
                    {
                        await foreach (var hydrated in FlushWindowAsync())
                        {
                            yield return hydrated;
                        }
                    }
                }

                await foreach (var hydrated in FlushWindowAsync())
                {
                    yield return hydrated;
                }
            }
            finally
            {
                // Observe any still-in-flight MoveNextAsync before disposal. Otherwise an
                // exception from FlushWindowAsync would race teardown and surface as
                // NotSupportedException from the compiler-generated DisposeAsync.
                if (moveTask is { } pending)
                {
                    // Cancel via the linked child so we don't wait out the polling interval.
                    // Caller's token is untouched.
                    enumeratorCts.Cancel();
                    try
                    {
                        await pending;
                    }
                    catch
                    {
                        // Outer exception is already propagating; teardown signals are expected.
                    }
                }
            }
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async Task<T> HydrateBatchChildAsync<T>(JobRun run, ChildOutputBuffer? buffer,
        CancellationToken cancellationToken)
    {
        await ThrowIfNonSuccessTerminalAsync(run, cancellationToken);

        var target = typeof(T);

        if (TypeHelpers.TryGetCollectionElementType(target, out var itemType, out var asArray))
        {
            var method = typeof(JobClient)
                .GetMethod(nameof(HydrateBatchChildCollectionAsync), BindingFlags.Instance | BindingFlags.NonPublic)!
                .MakeGenericMethod(itemType);
            var task = (Task<object?>)method.Invoke(this, [run, buffer, asArray, cancellationToken])!;
            return (T)(await task)!;
        }

        return await HydrateScalarWithBufferAsync<T>(run, buffer, cancellationToken);
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private Task<T> HydrateScalarWithBufferAsync<T>(JobRun run, ChildOutputBuffer? buffer, CancellationToken _)
    {
        if (run.Result is { } resultJson)
        {
            return Task.FromResult(JsonSerializer.Deserialize<T>(resultJson, _serializerOptions)!);
        }

        if (buffer is { OutputCompleted: true, Attempt: var attempt } && attempt == run.Attempt)
        {
            return Task.FromResult(buffer.Payloads.Count switch
            {
                0 => throw new InvalidOperationException($"Run '{run.Id}' produced no result."),
                1 => JsonSerializer.Deserialize<T>(buffer.Payloads[0], _serializerOptions)!,
                _ => throw new InvalidOperationException(
                    $"Run '{run.Id}' produced {buffer.Payloads.Count} items; cannot materialize as scalar.")
            });
        }

        throw new InvalidOperationException($"Run '{run.Id}' produced no result.");
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private Task<object?> HydrateBatchChildCollectionAsync<TElement>(JobRun run, ChildOutputBuffer? buffer,
        bool asArray, CancellationToken _)
    {
        List<TElement> items;

        if (run.Result is { } resultJson)
        {
            items = JsonSerializer.Deserialize<List<TElement>>(resultJson, _serializerOptions) ?? [];
        }
        else if (buffer is { OutputCompleted: true, Attempt: var attempt } && attempt == run.Attempt)
        {
            items = new(buffer.Payloads.Count);
            foreach (var payload in buffer.Payloads)
            {
                items.Add(JsonSerializer.Deserialize<TElement>(payload, _serializerOptions)!);
            }
        }
        else
        {
            // OutputComplete is written before the Succeeded status event in commit order, so by
            // the time we hydrate a succeeded child our buffer has seen both. Reaching this branch
            // means the run truly produced no result.
            throw new InvalidOperationException($"Run '{run.Id}' produced no result.");
        }

        return Task.FromResult<object?>(asArray ? items.ToArray() : items);
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async IAsyncEnumerable<HydratedChild<IAsyncEnumerable<TElement>>> StreamBatchLiveInnerAsync<TElement>(
        string batchId, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        // The event-reading loop MUST run on a background task. If it ran inline, the outer
        // iterator would suspend while a consumer reads an inner stream, and the inner stream's
        // channel would never get written to (deadlock). So a background task pumps
        // ObserveBatchEventsAsync into per-child channels, and the outer iterator just reads
        // "new child stream" handles from a top-level channel.
        var outerChannel = Channel.CreateUnbounded<HydratedChild<IAsyncEnumerable<TElement>>>(
            new() { SingleReader = true, SingleWriter = true });

        using var pumpCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var pump = Task.Run(() => PumpBatchLiveInnerAsync(batchId, outerChannel.Writer, pumpCts.Token),
            pumpCts.Token);

        try
        {
            await foreach (var handle in outerChannel.Reader.ReadAllAsync(cancellationToken))
            {
                yield return handle;
            }
        }
        finally
        {
            pumpCts.Cancel();
            try
            {
                await pump;
            }
            catch
            {
                /* surfaced via channel */
            }
        }
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async Task PumpBatchLiveInnerAsync<TElement>(string batchId,
        ChannelWriter<HydratedChild<IAsyncEnumerable<TElement>>> outerWriter,
        CancellationToken cancellationToken)
    {
        // emitted dedupes outer yields against late events arriving after a child terminated.
        var live = new Dictionary<string, ChildLiveStream<TElement>>(StringComparer.Ordinal);
        var emitted = new HashSet<string>(StringComparer.Ordinal);

        try
        {
            await foreach (var @event in ObserveBatchEventsAsync(batchId, 0, cancellationToken))
            {
                if (!live.TryGetValue(@event.RunId, out var child))
                {
                    if (!emitted.Add(@event.RunId))
                    {
                        continue;
                    }

                    child = new(@event.RunId, @event.Attempt, _serializerOptions, logger);
                    live[@event.RunId] = child;
                    await outerWriter.WriteAsync(new(child.Reader, null), cancellationToken);
                }

                switch (@event.EventType)
                {
                    case RunEventType.Output:
                        child.PushOutput(@event);
                        break;
                    case RunEventType.OutputComplete:
                        child.MarkOutputComplete(@event.Attempt);
                        break;
                    case RunEventType.Status when RunStatusEvents.TryGetStatus(@event, out var status)
                                                  && status.IsTerminal:
                        // Child failure surfaces on that child's next MoveNextAsync; other inner
                        // streams continue unaffected.
                        var snapshot = await store.GetRunAsync(@event.RunId, cancellationToken);
                        if (snapshot is null)
                        {
                            child.CompleteNotFound();
                        }
                        else if (snapshot.Status is JobStatus.Failed or JobStatus.Canceled)
                        {
                            // Enrich with AttemptFailure detail when available, but fall back to
                            // a minimal exception on a store hiccup so iteration still faults cleanly.
                            JobRunException faulted;
                            try
                            {
                                faulted = await BuildJobRunExceptionAsync(snapshot, cancellationToken);
                            }
                            catch (Exception ex) when (ex is not OperationCanceledException)
                            {
                                faulted = new(snapshot.Id, snapshot.Status, snapshot.Reason);
                            }

                            child.CompleteFaulted(faulted);
                        }
                        else
                        {
                            child.Complete();
                        }

                        live.Remove(@event.RunId);
                        break;
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Consumer dropped the iterator; fault in-flight inner streams so iteration unblocks.
            foreach (var child in live.Values)
            {
                child.CompleteFaulted(new OperationCanceledException(cancellationToken));
            }

            outerWriter.TryComplete();
            throw;
        }
        catch (Exception ex)
        {
            foreach (var child in live.Values)
            {
                child.CompleteFaulted(ex);
            }

            outerWriter.TryComplete(ex);
            throw;
        }

        foreach (var child in live.Values)
        {
            child.Complete();
        }

        outerWriter.TryComplete();
    }

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

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private async IAsyncEnumerable<TItem> OwnedRunStreamAsync<TItem>(string runId,
        [EnumeratorCancellation] CancellationToken ownerCancellationToken)
    {
        await using var enumerator = StreamRunHydratedAsync<TItem>(runId, ownerCancellationToken)
            .GetAsyncEnumerator(ownerCancellationToken);
        var completed = false;

        try
        {
            while (true)
            {
                TItem item;
                if (!await enumerator.MoveNextAsync())
                {
                    completed = true;
                    yield break;
                }

                item = enumerator.Current;

                yield return item;
            }
        }
        finally
        {
            if (!completed)
            {
                await TryCancelOwnedRunAsync(runId);
            }
        }
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
            DeduplicationId = runOptions.DeduplicationId,
            RerunOfRunId = rerunOfRunId,
            Progress = 0,
            Attempt = 0,
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

    [RequiresUnreferencedCode("Reflects over user-supplied argument objects.")]
    [RequiresDynamicCode("Reflects over user-supplied argument objects.")]
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
                Payload = JsonSerializer.Serialize(declaration, SurefireJsonContext.Default.InputDeclarationEnvelope),
                CreatedAt = timeProvider.GetUtcNow(),
                Attempt = 0
            }
        ];
    }

    [RequiresUnreferencedCode("Reflects over runtime types to detect IAsyncEnumerable.")]
    [RequiresDynamicCode("Reflects over runtime types to detect IAsyncEnumerable.")]
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

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
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

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
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

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
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

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
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
        await eventWriter.EnqueueAsync(
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

    private async Task TryCancelOwnedRunAsync(string runId)
    {
        using var cleanupCts = new CancellationTokenSource(options.ShutdownTimeout);
        try
        {
            await runCancellation.CancelRunAndDescendantsAsync(runId, OwnedOperationCancellationReason,
                cleanupCts.Token);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not AccessViolationException)
        {
            Log.FailedToPropagateCancellation(logger, ex, runId);
        }
    }

    private async Task TryCancelOwnedBatchAsync(string batchId)
    {
        using var cleanupCts = new CancellationTokenSource(options.ShutdownTimeout);
        try
        {
            await CancelBatchAsync(batchId, OwnedOperationCancellationReason, cleanupCts.Token);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not AccessViolationException)
        {
            Log.FailedToPropagateCancellation(logger, ex, batchId);
        }
    }

    private async Task TryCancelRunBestEffortAsync(string runId)
    {
        using var cleanupCts = new CancellationTokenSource(options.ShutdownTimeout);
        try
        {
            await CancelAsync(runId, cleanupCts.Token);
        }
        catch (Exception ex) when (ex is not OutOfMemoryException and not AccessViolationException)
        {
            Log.FailedBestEffortCancellation(logger, ex, runId);
        }
    }

    private static Task ReleaseWakeupAsync(SemaphoreSlim wakeup) => WakeupSignal.ReleaseAsync(wakeup);

    private Task WaitForWakeupAsync(SemaphoreSlim wakeup, CancellationToken cancellationToken) =>
        WakeupSignal.WaitAsync(wakeup, options.PollingInterval, cancellationToken);

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
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

    /// <summary>
    ///     Unified batch hydration carrying either a successfully-hydrated T or the failure exception
    ///     for a non-success child. <c>StreamBatchAsync</c> throws on first failure;
    ///     <c>WaitBatchAsync</c> collects them for a terminal <see cref="AggregateException" />.
    /// </summary>
    private readonly struct HydratedChild<T>(T? value, Exception? exception)
    {
        public T? Value { get; } = value;
        public Exception? Exception { get; } = exception;
    }

    private sealed class ChildOutputBuffer
    {
        public int Attempt { get; set; }
        public List<string> Payloads { get; } = new();
        public bool OutputCompleted { get; set; }
    }

    private sealed class ChildLiveStream<TElement>
    {
        private readonly Channel<TElement> _channel =
            Channel.CreateUnbounded<TElement>(
                new() { SingleReader = true, SingleWriter = true });

        private readonly ILogger _logger;
        private readonly string _runId;
        private readonly JsonSerializerOptions _serializer;

        private int _attempt;

        public ChildLiveStream(string runId, int attempt, JsonSerializerOptions serializer, ILogger logger)
        {
            _runId = runId;
            _attempt = attempt;
            _serializer = serializer;
            _logger = logger;
            Reader = _channel.Reader.ReadAllAsync();
        }

        public IAsyncEnumerable<TElement> Reader { get; }

        [RequiresUnreferencedCode("Uses JSON deserialization.")]
        [RequiresDynamicCode("Uses JSON deserialization.")]
        public void PushOutput(RunEvent @event)
        {
            if (@event.Attempt < _attempt)
            {
                return;
            }

            if (@event.Attempt > _attempt)
            {
                // Retry-transparent: items from prior attempts are already visible to the consumer.
                _attempt = @event.Attempt;
            }

            try
            {
                var value = JsonSerializer.Deserialize<TElement>(@event.Payload, _serializer);
                if (value is null)
                {
                    Log.DeserializationReturnedNull(_logger, _runId, @event.Id, @event.EventType,
                        "batch live stream");
                    return;
                }

                _channel.Writer.TryWrite(value);
            }
            catch (JsonException ex)
            {
                Log.DeserializationFailed(_logger, ex, _runId, @event.Id, @event.EventType, "batch live stream");
            }
        }

        public void MarkOutputComplete(int attempt)
        {
            if (attempt >= _attempt)
            {
                _attempt = attempt;
            }
        }

        public void Complete() => _channel.Writer.TryComplete();

        public void CompleteFaulted(Exception exception) => _channel.Writer.TryComplete(exception);

        public void CompleteNotFound() =>
            _channel.Writer.TryComplete(new RunNotFoundException(_runId));
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
