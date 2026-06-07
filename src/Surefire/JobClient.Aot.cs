using System.Buffers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Surefire;

/// <summary>
///     AOT-clean overloads of <see cref="IJobClient" /> methods. These take pre-serialized
///     <see cref="RunArguments" /> and explicit <see cref="JsonTypeInfo" /> for result
///     deserialization, so no reflection is needed at runtime.
/// </summary>
internal sealed partial class JobClient
{
    public async Task<JobRun> TriggerAsync(string job, RunArguments? args, RunOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var runOptions = options ?? new();
        string? derivedId = null;
        var durableStep = 0;
        string? orchestratorRunId = null;
        if (JobContext.Current is { OrchestratorRunId: { } orchId } ctx)
        {
            var (step, isReplay) = ctx.AllocateStep();
            orchestratorRunId = orchId;
            durableStep = step;
            derivedId = DurableIds.DerivedRunId(orchId, durableStep);
            if (isReplay)
            {
                ctx.ValidateReplayChildRun(durableStep, derivedId);
            }

            // Claim-time snapshot serves the existence check from memory during replay; only fall
            // through to the store for runs that aren't in the orchestrator's recorded set (rare
            // case: handler made a change to the derived id strategy across replays, won't happen
            // for deterministic handlers, but the store remains the source of truth).
            var existing = ctx.DurableSnapshot is { } snap
                           && snap.Children.TryGetValue(derivedId, out var snapChild)
                ? snapChild
                : await store.GetRunAsync(derivedId, cancellationToken);
            if (existing is { })
            {
                // If the run's input pump never wrote InputComplete for one or more declared
                // streams (host crashed mid-pump), advance the source past the already-recorded
                // sequence and restart the pump. Without this, the child awaits an InputComplete
                // that nobody will ever write.
                if ((args?.Streams.Count ?? 0) > 0)
                {
                    await ResumeAotInputPumpIfNeededAsync(existing.Id, args!.Streams, cancellationToken);
                }

                return existing;
            }

            runOptions = runOptions with
            {
                DeduplicationId = runOptions.DeduplicationId
                                  ?? DurableIds.DedupId(orchId, durableStep)
            };
        }

        var (requestedPriority, isDurable) =
            await ResolveTriggerMetadataAsync(job, runOptions.Priority, cancellationToken);
        var resolvedArgs = args ?? RunArguments.Empty;
        var argumentsJson = MaterializeJson(resolvedArgs);
        var run = CreateRun(job, argumentsJson, runOptions, timeProvider.GetUtcNow(),
            requestedPriority ?? 0, derivedId, isDurable: isDurable);
        var initialEvents = BuildInitialEventsFromAot(run.Id, resolvedArgs.Streams);

        // Atomically update the orchestrator's highest_recorded_step alongside the child run
        // insert, so a crash between the two leaves no inconsistent state. The replay reads
        // HighestRecordedStep from the orchestrator row at claim time.
        DurableStepRecord? stepRecord = orchestratorRunId is { } recordingOrchId
            ? new DurableStepRecord(recordingOrchId, durableStep)
            : null;

        if (!await store.TryCreateRunAsync(run, initialEvents: initialEvents,
                durableStepRecord: stepRecord, cancellationToken: cancellationToken))
        {
            var existing = await store.GetRunAsync(run.Id, cancellationToken);
            if (existing is { })
            {
                return existing;
            }

            throw new RunConflictException(run.Id, $"Run '{run.Id}' already exists.");
        }

        await notifications.PublishAsync(NotificationChannels.RunCreated, null, cancellationToken);
        if (resolvedArgs.Streams.Count > 0)
        {
            StartAotInputPump(run.Id, resolvedArgs.Streams);
        }

        return run;
    }

    public async Task<T> WaitAsync<T>(string runId, CancellationToken cancellationToken = default)
    {
        var resultTypeInfo = ResolveTypeInfo<T>();
        var run = await WaitAsync(runId, cancellationToken);
        await ThrowIfNonSuccessTerminalAsync(run, cancellationToken);
        return await HydrateScalarAotAsync(run, resultTypeInfo, cancellationToken);
    }

    public IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, CancellationToken cancellationToken = default)
    {
        var elementTypeInfo = ResolveTypeInfo<T>();
        return StreamRunHydratedAotAsync(runId, elementTypeInfo, cancellationToken);
    }

    public async Task<T> RunAsync<T>(string job, RunArguments? args, RunOptions? options = null,
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

    public async Task RunAsync(string job, RunArguments? args, RunOptions? options = null,
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

    public async IAsyncEnumerable<T> StreamAsync<T>(string job, RunArguments? args, RunOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var elementTypeInfo = ResolveTypeInfo<T>();
        var run = await TriggerAsync(job, args, options, cancellationToken);
        await using var enumerator = StreamRunHydratedAotAsync(run.Id, elementTypeInfo, cancellationToken)
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
                    // abandonment. Don't cancel the run we just created - the orchestrator will
                    // resume and re-enter this iterator with the recorded result available.
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
                await TryCancelOwnedRunAsync(run.Id);
            }
        }
    }

    public async Task<JobBatch> TriggerBatchAsync(string job, IEnumerable<RunArguments?> args,
        BatchRunOptions? options = null, CancellationToken cancellationToken = default) =>
        await TriggerBatchAsyncAotCore(job, args, options, cancellationToken);

    public async Task<IReadOnlyList<T>> WaitBatchAsync<T>(string batchId,
        CancellationToken cancellationToken = default)
    {
        var resultTypeInfo = ResolveTypeInfo<T>();
        // Aggregate semantics: every child runs to completion regardless of individual failures.
        // Collect successful results and aggregate per-child exceptions at the end.
        var results = new List<T>();
        var failures = new List<Exception>();
        await foreach (var hydrated in HydrateBatchChildrenAotAsync(batchId, resultTypeInfo, false,
                           cancellationToken))
        {
            if (hydrated.Exception is { } ex)
            {
                failures.Add(ex);
            }
            else
            {
                results.Add(hydrated.Value!);
            }
        }

        if (failures.Count > 0)
        {
            throw new AggregateException(failures);
        }

        return results;
    }

    public async IAsyncEnumerable<T> WaitEachAsync<T>(string batchId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var resultTypeInfo = ResolveTypeInfo<T>();
        // Fail-fast: throw on the first child terminal failure as soon as it's yielded.
        await foreach (var hydrated in HydrateBatchChildrenAotAsync(batchId, resultTypeInfo, true,
                           cancellationToken))
        {
            yield return hydrated.Value!;
        }
    }

    public async Task<IReadOnlyList<T>> RunBatchAsync<T>(string job, IEnumerable<RunArguments?> args,
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

    public async IAsyncEnumerable<T> StreamBatchAsync<T>(string job, IEnumerable<RunArguments?> args,
        BatchRunOptions? options = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var batch = await TriggerBatchAsync(job, args, options, cancellationToken);
        await using var enumerator = WaitEachAsync<T>(batch.Id, cancellationToken)
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
                    // resume and re-enter this iterator with the recorded children terminal.
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
                await TryCancelOwnedBatchAsync(batch.Id);
            }
        }
    }

    private JsonTypeInfo<T> ResolveTypeInfo<T>() =>
        (JsonTypeInfo<T>)_serializerOptions.GetTypeInfo(typeof(T));

    private async IAsyncEnumerable<HydratedChild<T>> HydrateBatchChildrenAotAsync<T>(string batchId,
        JsonTypeInfo<T> resultTypeInfo, bool throwOnChildFailure,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var child in WaitEachAsync(batchId, cancellationToken))
        {
            HydratedChild<T> hydrated;
            try
            {
                if (child.Status is JobStatus.Failed or JobStatus.Canceled)
                {
                    throw await BuildJobRunExceptionAsync(child, cancellationToken);
                }

                var value = await HydrateScalarAotAsync(child, resultTypeInfo, cancellationToken);
                hydrated = new(value, null);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                hydrated = new(default, ex);
            }

            if (hydrated.Exception is { } ex2 && throwOnChildFailure)
            {
                throw ex2;
            }

            yield return hydrated;
        }
    }

    // ---------- Internal AOT helpers ----------

    private string? MaterializeJson(RunArguments args)
    {
        if (args.WriteJson is { } writeJson)
        {
            var buffer = new ArrayBufferWriter<byte>();
            using (var writer = new Utf8JsonWriter(buffer))
            {
                writeJson(_serializerOptions, writer);
            }

            return Encoding.UTF8.GetString(buffer.WrittenSpan);
        }

        return args.Json;
    }

    private IReadOnlyList<RunEvent> BuildInitialEventsFromAot(string runId,
        IReadOnlyList<RunArgumentStream> streams)
    {
        if (streams.Count == 0)
        {
            return [];
        }

        var declaration = new InputDeclarationEnvelope
        {
            Arguments = streams.Select(s => s.ArgumentName).ToArray()
        };
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

    private void StartAotInputPump(string runId, IReadOnlyList<RunArgumentStream> streams)
    {
        var pumpCts = new CancellationTokenSource();
        if (!_inputPumpTokens.TryAdd(runId, pumpCts))
        {
            pumpCts.Dispose();
            throw new InvalidOperationException($"An input pump is already active for run '{runId}'.");
        }

        _ = MonitorAotInputPumpAsync(runId, streams, pumpCts);
    }

    // Inspect pump state for a replayed durable run: if any declared stream is missing its
    // InputComplete event, advance the corresponding caller source past the already-recorded
    // Sequence and restart only that stream's pump. Skips work entirely when the previous
    // pump finished cleanly (all InputComplete events present), which is the common case.
    private async Task ResumeAotInputPumpIfNeededAsync(string runId,
        IReadOnlyList<RunArgumentStream> streams, CancellationToken cancellationToken)
    {
        var pumpState = await store.GetInputPumpStateAsync(runId, cancellationToken);
        if (pumpState.Count > 0 && pumpState.Values.All(s => s.InputComplete))
        {
            return;
        }

        var resumeStreams = new List<RunArgumentStream>(streams.Count);
        foreach (var stream in streams)
        {
            if (pumpState.TryGetValue(stream.ArgumentName, out var state) && state.InputComplete)
            {
                continue;
            }

            resumeStreams.Add(new()
            {
                ArgumentName = stream.ArgumentName,
                SerializeItems = stream.SerializeItems,
                ResumeFromSequence = state.LastSequence
            });
        }

        if (resumeStreams.Count == 0)
        {
            return;
        }

        // _inputPumpTokens is a per-runId guard: if a pump is somehow already active for this
        // run on this process (it shouldn't be on replay), the existing pump owns the streams.
        if (_inputPumpTokens.ContainsKey(runId))
        {
            return;
        }

        StartAotInputPump(runId, resumeStreams);
    }

    private async Task ResumeBatchInputPumpsIfNeededAsync(string batchId,
        IReadOnlyList<(string JobName, RunArguments Args, BatchRunOptions? Options)> items,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < items.Count; i++)
        {
            var streams = items[i].Args.Streams;
            if (streams.Count == 0)
            {
                continue;
            }

            var childId = DurableIds.DerivedBatchChildRunId(batchId, i);
            await ResumeAotInputPumpIfNeededAsync(childId, streams, cancellationToken);
        }
    }

    private async Task MonitorAotInputPumpAsync(string runId, IReadOnlyList<RunArgumentStream> streams,
        CancellationTokenSource pumpCts)
    {
        // Cancel the pump when the run terminates (success, failure, or cancellation) so streams
        // don't outlive their consumer. Cleanup of the channel subscription and the CTS itself
        // happens in the finally block whether or not the run terminated.
        await using var subscription = await notifications.SubscribeAsync(
            NotificationChannels.RunTerminated(runId),
            _ =>
            {
                try
                {
                    pumpCts.Cancel();
                }
                catch (ObjectDisposedException)
                {
                }

                return Task.CompletedTask;
            },
            pumpCts.Token);

        try
        {
            var pumpTasks = streams.Select(s => PumpAotStreamAsync(runId, s, pumpCts.Token)).ToList();
            var failures = new List<Exception>();
            foreach (var task in pumpTasks)
            {
                try
                {
                    await task;
                }
                catch (Exception ex)
                {
                    failures.Add(ex);
                    Log.InputStreamingFailed(logger, ex, runId);
                }
            }

            if (failures.Count > 0)
            {
                throw new AggregateException(failures);
            }
        }
        catch (Exception ex) when (ex is not AggregateException)
        {
            Log.InputStreamingFailed(logger, ex, runId);
        }
        finally
        {
            _inputPumpTokens.TryRemove(runId, out _);
            pumpCts.Dispose();
        }
    }


    private async Task PumpAotStreamAsync(string runId, RunArgumentStream stream,
        CancellationToken cancellationToken)
    {
        // On replay (ResumeFromSequence > 0), skip items the prior attempt already wrote and
        // continue numbering past LastSequence so the event log stays monotonic.
        var skip = stream.ResumeFromSequence;
        var sequence = skip;
        long emitted = 0;
        try
        {
            await foreach (var serialized in stream.SerializeItems(_serializerOptions)
                               .WithCancellation(cancellationToken))
            {
                emitted++;
                if (emitted <= skip)
                {
                    continue;
                }

                sequence++;
                if (!await AppendInputEventAsync(runId, RunEventType.Input, new()
                    {
                        Argument = stream.ArgumentName,
                        Sequence = sequence,
                        Payload = serialized,
                        IsComplete = false,
                        Error = null
                    }, cancellationToken))
                {
                    return;
                }
            }

            await AppendInputEventAsync(runId, RunEventType.InputComplete, new()
            {
                Argument = stream.ArgumentName,
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
                Argument = stream.ArgumentName,
                Sequence = sequence + 1,
                Payload = null,
                IsComplete = true,
                Error = cancellationToken.IsCancellationRequested ? null : ex.Message
            }, CancellationToken.None);
        }
        catch (Exception ex)
        {
            await AppendInputEventAsync(runId, RunEventType.InputComplete, new()
            {
                Argument = stream.ArgumentName,
                Sequence = sequence + 1,
                Payload = null,
                IsComplete = true,
                Error = ex.Message
            }, CancellationToken.None);
            throw;
        }
    }

    private async Task<T> HydrateScalarAotAsync<T>(JobRun run, JsonTypeInfo<T> typeInfo,
        CancellationToken cancellationToken)
    {
        if (run.Result is { } resultJson)
        {
            return JsonSerializer.Deserialize(resultJson, typeInfo)!;
        }

        if (await HasOutputCompleteForAttemptAsync(run.Id, run.Attempt, cancellationToken))
        {
            var items = await ReadAttemptOutputPayloadsAsync(run.Id, run.Attempt, cancellationToken);
            if (items.Count == 0)
            {
                throw new InvalidOperationException($"Run '{run.Id}' produced no result.");
            }

            if (items.Count == 1)
            {
                return JsonSerializer.Deserialize(items[0], typeInfo)!;
            }

            // Multiple Output items materialize into a collection-typed T (List<U>/U[]/...).
            // The user's resolver chain must include T; in AOT contexts that means the
            // JsonSerializerContext registers the collection type explicitly.
            if (TryHydrateCollectionAot(items, out T? collection))
            {
                return collection!;
            }

            throw new InvalidOperationException(
                $"Run '{run.Id}' produced {items.Count} items but '{typeof(T)}' is not a supported collection type. " +
                "Use IAsyncEnumerable<U>, List<U>, U[], or register the collection type with your JsonSerializerContext.");
        }

        throw new InvalidOperationException($"Run '{run.Id}' produced no result.");
    }

    private bool TryHydrateCollectionAot<T>(IReadOnlyList<string> items, out T? collection)
    {
        var arrayJson = "[" + string.Join(",", items) + "]";
        try
        {
            var ti = ResolveTypeInfo<T>();
            collection = JsonSerializer.Deserialize(arrayJson, ti);
            return true;
        }
        catch (NotSupportedException)
        {
            collection = default;
            return false;
        }
        catch (JsonException)
        {
            collection = default;
            return false;
        }
    }

    private async IAsyncEnumerable<T> StreamRunHydratedAotAsync<T>(string runId, JsonTypeInfo<T> typeInfo,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        long sinceEventId = 0;
        var yieldedAny = false;

        await foreach (var @event in ObserveRunEventsAsync(runId, sinceEventId, cancellationToken))
        {
            sinceEventId = @event.Id;
            if (@event.EventType != RunEventType.Output)
            {
                continue;
            }

            T? item;
            try
            {
                item = JsonSerializer.Deserialize(@event.Payload, typeInfo);
            }
            catch (JsonException ex)
            {
                Log.DeserializationFailed(logger, ex, runId, @event.Id, @event.EventType, "run output stream");
                continue;
            }

            yieldedAny = true;
            if (item is { })
            {
                yield return item;
            }
        }

        var run = await store.GetRunAsync(runId, cancellationToken) ?? throw new RunNotFoundException(runId);
        if (run.Status is JobStatus.Failed or JobStatus.Canceled)
        {
            throw await BuildJobRunExceptionAsync(run, cancellationToken);
        }

        // Succeeded with no Output events but a stored result: mirror the reflection path's
        // semantics. Try to materialize the result JSON as a collection first; if that doesn't
        // parse, fall back to deserializing as a single scalar of T.
        if (!yieldedAny && run.Result is { } resultJson)
        {
            JsonTypeInfo<List<T>>? listTypeInfo;
            try
            {
                listTypeInfo = (JsonTypeInfo<List<T>>)_serializerOptions.GetTypeInfo(typeof(List<T>));
            }
            catch (NotSupportedException)
            {
                // The user's source-gen context doesn't include List<T>; fall through to scalar.
                listTypeInfo = null;
            }

            if (listTypeInfo is { })
            {
                List<T>? items = null;
                try
                {
                    items = JsonSerializer.Deserialize(resultJson, listTypeInfo);
                }
                catch (JsonException)
                {
                    // Result wasn't a JSON array; treat as scalar.
                }

                if (items is { })
                {
                    foreach (var item in items)
                    {
                        yield return item;
                    }

                    yield break;
                }
            }

            yield return JsonSerializer.Deserialize(resultJson, typeInfo)!;
        }
    }

    private async Task<JobBatch> TriggerBatchAsyncAotCore(string job, IEnumerable<RunArguments?> args,
        BatchRunOptions? options, CancellationToken cancellationToken)
    {
        var meta = await LookupJobMetadataAsync(job, cancellationToken);
        var priorityByJob = new Dictionary<string, int?>(StringComparer.Ordinal) { [job] = meta.Priority };
        var durableByJob = new Dictionary<string, bool>(StringComparer.Ordinal) { [job] = meta.IsDurable };
        var normalized = args.Select(a => (job, a ?? RunArguments.Empty, options)).ToList();
        return await TriggerBatchCoreAsync(normalized, priorityByJob, durableByJob, cancellationToken);
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
}
