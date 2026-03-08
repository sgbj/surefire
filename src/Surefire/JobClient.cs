using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace Surefire;

internal sealed class JobClient(IJobStore store, INotificationProvider notifications, JobRegistry registry, SurefireOptions options, TimeProvider timeProvider) : IJobClient
{
    public Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default)
        => store.GetRunAsync(runId, cancellationToken);

    public Task<string> TriggerAsync(string jobName, object? args = null, CancellationToken cancellationToken = default)
        => TriggerAsync(jobName, args, null!, cancellationToken);

    public async Task<string> TriggerAsync(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default)
    {
        var (argsJson, streamingArgs) = ExtractStreamingArgs(args);
        var run = CreateRun(jobName, argsJson, options: options);
        await store.CreateRunAsync(run, cancellationToken);
        await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);

        if (streamingArgs is not null)
            StartInputPumps(run.Id, streamingArgs, CancellationToken.None);

        return run.Id;
    }

    public async Task CancelAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken)
            ?? throw new InvalidOperationException($"Run '{runId}' not found.");

        if (run.Status.IsTerminal)
            return;

        var now = timeProvider.GetUtcNow();

        if (run.Status == JobStatus.Pending)
        {
            // Atomically transition Pending → Cancelled to avoid TOCTOU with ClaimRunAsync.
            // Build a separate update object to avoid mutating the store's reference (InMemory same-object CAS).
            var update = new JobRun
            {
                Id = run.Id, JobName = run.JobName,
                Status = JobStatus.Cancelled, CompletedAt = now, CancelledAt = now
            };
            if (await store.TryUpdateRunStatusAsync(update, JobStatus.Pending, cancellationToken))
            {
                var payload = JsonSerializer.Serialize(new { status = (int)JobStatus.Cancelled }, options.SerializerOptions);
                var evt = new RunEvent
                {
                    RunId = runId,
                    EventType = RunEventType.Status,
                    Payload = payload,
                    CreatedAt = now
                };
                await store.AppendEventAsync(evt, cancellationToken);
                await notifications.PublishAsync(NotificationChannels.RunEvent(runId), "", cancellationToken);
                await notifications.PublishAsync(NotificationChannels.RunCompleted(runId), "", cancellationToken);
                await CancelDescendantsAsync(runId, cancellationToken);
                return;
            }
            // CAS failed: run was claimed between our read and CAS — fall through to Running path
        }

        await notifications.PublishAsync(NotificationChannels.RunCancel(runId), "", cancellationToken);
        await CancelDescendantsAsync(runId, cancellationToken);
    }

    private async Task CancelDescendantsAsync(string rootRunId, CancellationToken cancellationToken)
    {
        var queue = new Queue<string>();
        queue.Enqueue(rootRunId);

        while (queue.Count > 0)
        {
            var parentId = queue.Dequeue();
            var skip = 0;
            const int batchSize = 500;
            PagedResult<JobRun> children;
            do
            {
                children = await store.GetRunsAsync(
                    new RunFilter { ParentRunId = parentId, Skip = skip, Take = batchSize }, cancellationToken);
                skip += batchSize;

                foreach (var child in children.Items)
                {
                    // Always enqueue to find grandchildren — even terminal children may have
                    // non-terminal grandchildren (e.g., child B completed but triggered C via TriggerAsync)
                    queue.Enqueue(child.Id);

                    if (child.Status.IsTerminal) continue;

                    var now = timeProvider.GetUtcNow();
                    if (child.Status == JobStatus.Pending)
                    {
                        var update = new JobRun
                        {
                            Id = child.Id, JobName = child.JobName,
                            Status = JobStatus.Cancelled, CompletedAt = now, CancelledAt = now
                        };
                        if (await store.TryUpdateRunStatusAsync(update, JobStatus.Pending, cancellationToken))
                        {
                            var payload = JsonSerializer.Serialize(
                                new { status = (int)JobStatus.Cancelled }, options.SerializerOptions);
                            var evt = new RunEvent
                            {
                                RunId = child.Id, EventType = RunEventType.Status,
                                Payload = payload, CreatedAt = now
                            };
                            await store.AppendEventAsync(evt, cancellationToken);
                            await notifications.PublishAsync(
                                NotificationChannels.RunEvent(child.Id), "", cancellationToken);
                            await notifications.PublishAsync(
                                NotificationChannels.RunCompleted(child.Id), "", cancellationToken);
                        }
                        else
                        {
                            // CAS failed: child was claimed (Pending→Running) between read and CAS.
                            await notifications.PublishAsync(
                                NotificationChannels.RunCancel(child.Id), "", cancellationToken);
                        }
                    }
                    else // Running
                    {
                        await notifications.PublishAsync(
                            NotificationChannels.RunCancel(child.Id), "", cancellationToken);
                    }
                }
            } while (children.Items.Count == batchSize);
        }
    }

    public async Task<string> RerunAsync(string runId, CancellationToken cancellationToken = default)
    {
        var run = await store.GetRunAsync(runId, cancellationToken)
            ?? throw new InvalidOperationException($"Run '{runId}' not found.");

        var now = timeProvider.GetUtcNow();
        var newRun = new JobRun
        {
            Id = Guid.CreateVersion7().ToString("N"),
            JobName = run.JobName,
            Arguments = run.Arguments,
            CreatedAt = now,
            NotBefore = now,
            Priority = run.Priority,
            ParentRunId = JobContext.Current.Value?.RunId,
            RerunOfRunId = run.RerunOfRunId ?? run.RetryOfRunId ?? run.Id
        };
        await store.CreateRunAsync(newRun, cancellationToken);
        await notifications.PublishAsync(NotificationChannels.RunCreated, newRun.Id, cancellationToken);
        return newRun.Id;
    }

    public Task<TResult> RunAsync<TResult>(string jobName, object? args = null, CancellationToken cancellationToken = default)
        => RunAsync<TResult>(jobName, args, null!, cancellationToken);

    public async Task<TResult> RunAsync<TResult>(string jobName, object? args, RunOptions runOptions, CancellationToken cancellationToken = default)
    {
        var (argsJson, streamingArgs) = ExtractStreamingArgs(args);
        var runId = Guid.CreateVersion7().ToString("N");
        var run = CreateRun(jobName, argsJson, runId, runOptions);
        await store.CreateRunAsync(run, cancellationToken);
        await notifications.PublishAsync(NotificationChannels.RunCreated, run.Id, cancellationToken);

        CancellationTokenSource? pumpCts = null;
        if (streamingArgs is not null)
        {
            pumpCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            StartInputPumps(run.Id, streamingArgs, pumpCts.Token);
        }

        // Streaming output: return auto-following stream
        if (StreamingHelper.IsAsyncEnumerable(typeof(TResult)))
        {
            var itemType = typeof(TResult).GetGenericArguments()[0];
            var method = typeof(JobClient)
                .GetMethod(nameof(CreateAutoFollowStream), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(itemType);
            return (TResult)method.Invoke(this, [runId, pumpCts, cancellationToken])!;
        }

        // Non-streaming: same WatchRunAsync primitive with retry following
        var activeRunId = runId;
        try
        {
            while (true)
            {
                try
                {
                    await foreach (var _ in WatchRunAsync<object>(activeRunId, cancellationToken))
                    {
                        // Non-streaming jobs don't produce output events
                    }
                    // Completed — read result from store
                    var completedRun = await store.GetRunAsync(activeRunId, cancellationToken);
                    if (completedRun?.Result is null)
                        return default!;
                    return JsonSerializer.Deserialize<TResult>(completedRun.Result, options.SerializerOptions)!;
                }
                catch (JobRunFailedException ex) when (ex.RetryRunId is not null)
                {
                    activeRunId = ex.RetryRunId;
                    continue;
                }
            }
        }
        finally
        {
            if (pumpCts is not null)
            {
                try { await pumpCts.CancelAsync(); } catch (ObjectDisposedException) { }
                pumpCts.Dispose();
            }

            try { await notifications.PublishAsync(NotificationChannels.RunCancel(activeRunId), ""); }
            catch { /* best-effort */ }
        }
    }

    public async Task RunAsync(string jobName, object? args = null, CancellationToken cancellationToken = default)
        => await RunAsync<object?>(jobName, args, null!, cancellationToken);

    public async Task RunAsync(string jobName, object? args, RunOptions options, CancellationToken cancellationToken = default)
        => await RunAsync<object?>(jobName, args, options, cancellationToken);

    /// <summary>
    /// Creates an IAsyncEnumerable that watches a run's output events and auto-follows retries.
    /// Uses a Channel to bridge the retry-following logic (which requires try-catch)
    /// from the async iterator (which can only use try-finally with yield).
    /// </summary>
    private async IAsyncEnumerable<T> CreateAutoFollowStream<T>(string runId, CancellationTokenSource? pumpCts, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = Channel.CreateUnbounded<T>();
        var activeRunId = new[] { runId };
        using var followCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var followTask = FollowRetriesIntoChannelAsync<T>(runId, channel.Writer, activeRunId, followCts.Token);

        try
        {
            await foreach (var item in channel.Reader.ReadAllAsync(cancellationToken))
                yield return item;
        }
        finally
        {
            try { await followCts.CancelAsync(); } catch (ObjectDisposedException) { }
            try { await followTask; } catch { /* exceptions already propagated via channel */ }

            if (pumpCts is not null)
            {
                try { await pumpCts.CancelAsync(); } catch (ObjectDisposedException) { }
                pumpCts.Dispose();
            }

            try { await notifications.PublishAsync(NotificationChannels.RunCancel(activeRunId[0]), ""); }
            catch { /* best-effort */ }
        }
    }

    private async Task FollowRetriesIntoChannelAsync<T>(string runId, ChannelWriter<T> writer, string[] activeRunId, CancellationToken ct)
    {
        var currentRunId = runId;
        try
        {
            while (true)
            {
                try
                {
                    await foreach (var item in WatchRunAsync<T>(currentRunId, ct))
                        writer.TryWrite(item);
                    break;
                }
                catch (JobRunFailedException ex) when (ex.RetryRunId is not null)
                {
                    currentRunId = ex.RetryRunId;
                    activeRunId[0] = currentRunId;
                    continue;
                }
            }
            writer.Complete();
        }
        catch (Exception ex)
        {
            writer.Complete(ex);
        }
    }

    public async IAsyncEnumerable<T> WatchRunAsync<T>(string runId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var wakeup = new SemaphoreSlim(0, 1);
        var completed = false;

        await using var eventSub = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(runId),
            _ => { try { wakeup.Release(); } catch (Exception ex) when (ex is SemaphoreFullException or ObjectDisposedException) { } return Task.CompletedTask; },
            cancellationToken);

        await using var completedSub = await notifications.SubscribeAsync(
            NotificationChannels.RunCompleted(runId),
            _ => { completed = true; try { wakeup.Release(); } catch (Exception ex) when (ex is SemaphoreFullException or ObjectDisposedException) { } return Task.CompletedTask; },
            cancellationToken);

        // Check if already terminal before entering the loop
        var run = await store.GetRunAsync(runId, cancellationToken);
        if (run is not null && run.Status.IsTerminal)
            completed = true;

        long lastSeenId = 0;
        var outputErrored = false;

        while (!cancellationToken.IsCancellationRequested)
        {
            if (!outputErrored)
            {
                var events = await store.GetEventsAsync(runId, lastSeenId,
                    types: [RunEventType.Output, RunEventType.OutputComplete], cancellationToken: cancellationToken);

                foreach (var evt in events)
                {
                    lastSeenId = evt.Id;

                    if (evt.EventType == RunEventType.OutputComplete)
                    {
                        using var doc = JsonDocument.Parse(evt.Payload);
                        if (doc.RootElement.TryGetProperty("error", out var errorProp) && errorProp.ValueKind == JsonValueKind.String)
                        {
                            // Stream errored — fall through to terminal check for retryRunId
                            outputErrored = true;
                            break;
                        }
                        yield break;
                    }

                    yield return JsonSerializer.Deserialize<T>(evt.Payload, options.SerializerOptions)!;
                }
            }

            if (completed)
            {
                if (!outputErrored)
                {
                    // Drain remaining output events
                    var finalEvents = await store.GetEventsAsync(runId, lastSeenId,
                        types: [RunEventType.Output, RunEventType.OutputComplete], cancellationToken: cancellationToken);

                    foreach (var evt in finalEvents)
                    {
                        lastSeenId = evt.Id;

                        if (evt.EventType == RunEventType.OutputComplete)
                        {
                            using var doc = JsonDocument.Parse(evt.Payload);
                            if (doc.RootElement.TryGetProperty("error", out var errorProp) && errorProp.ValueKind == JsonValueKind.String)
                            {
                                outputErrored = true;
                                break;
                            }
                            yield break;
                        }

                        yield return JsonSerializer.Deserialize<T>(evt.Payload, options.SerializerOptions)!;
                    }
                }

                // Check terminal status — read retryRunId from last Status event (durable)
                run = await store.GetRunAsync(runId, cancellationToken);
                if (run is not null && run.Status == JobStatus.Cancelled)
                    throw new ChildRunCancelledException(runId);
                if (run is not null && run.Status != JobStatus.Completed)
                {
                    var retryRunId = await GetRetryRunIdFromEventsAsync(runId, cancellationToken);
                    throw new JobRunFailedException(runId, retryRunId, run.Error);
                }
                yield break;
            }

            // OutputComplete with error but RunCompleted hasn't arrived yet — wait for it
            await wakeup.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    /// <summary>
    /// Reads the retryRunId from the last Status event for a run.
    /// Status events are durable (persisted in the event store), unlike notification payloads.
    /// </summary>
    private async Task<string?> GetRetryRunIdFromEventsAsync(string runId, CancellationToken cancellationToken)
    {
        var statusEvents = await store.GetEventsAsync(runId, sinceId: 0,
            types: [RunEventType.Status], cancellationToken: cancellationToken);
        if (statusEvents.Count == 0)
            return null;
        var lastStatus = statusEvents[^1];
        using var doc = JsonDocument.Parse(lastStatus.Payload);
        if (doc.RootElement.TryGetProperty("retryRunId", out var retryProp) && retryProp.ValueKind == JsonValueKind.String)
            return retryProp.GetString();
        return null;
    }

    private (string? argsJson, List<(string name, object enumerable, Type itemType)>? streaming) ExtractStreamingArgs(object? args)
    {
        if (args is null)
            return (null, null);

        // JSON types from System.Text.Json are not POCOs — serialize directly
        if (args is JsonElement or JsonDocument or JsonNode)
            return (JsonSerializer.Serialize(args, options.SerializerOptions), null);

        var properties = args.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        List<(string name, object enumerable, Type itemType)>? streaming = null;
        JsonObject? jsonObj = null;

        foreach (var prop in properties)
        {
            var propType = prop.PropertyType;
            Type? asyncItemType = null;

            if (StreamingHelper.IsAsyncEnumerable(propType))
            {
                var iface = propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>)
                    ? propType
                    : propType.GetInterfaces().First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>));
                asyncItemType = iface.GetGenericArguments()[0];
            }

            if (asyncItemType is not null)
            {
                var value = prop.GetValue(args);
                if (value is not null)
                {
                    streaming ??= [];
                    streaming.Add((prop.Name, value, asyncItemType));
                }
                continue;
            }

            // Non-streaming property — add to JSON object
            jsonObj ??= new JsonObject();
            var jsonValue = JsonSerializer.SerializeToNode(prop.GetValue(args), prop.PropertyType, options.SerializerOptions);
            jsonObj[options.SerializerOptions.PropertyNamingPolicy?.ConvertName(prop.Name) ?? prop.Name] = jsonValue;
        }

        if (streaming is null)
        {
            // No streaming args — serialize normally for best compatibility
            return (JsonSerializer.Serialize(args, options.SerializerOptions), null);
        }

        var argsJson = jsonObj is not null ? jsonObj.ToJsonString(options.SerializerOptions) : null;
        return (argsJson, streaming);
    }

    private void StartInputPumps(string runId, List<(string name, object enumerable, Type itemType)> streamingArgs, CancellationToken ct)
    {
        foreach (var (name, enumerable, itemType) in streamingArgs)
        {
            var method = typeof(JobClient)
                .GetMethod(nameof(PumpInputCoreAsync), BindingFlags.NonPublic | BindingFlags.Instance)!
                .MakeGenericMethod(itemType);
            _ = Task.Run(() => (Task)method.Invoke(this, [runId, name, enumerable, ct])!, CancellationToken.None);
        }
    }

    private async Task PumpInputCoreAsync<T>(string runId, string paramName, IAsyncEnumerable<T> source, CancellationToken ct)
    {
        try
        {
            await foreach (var item in source.WithCancellation(ct))
            {
                var value = JsonSerializer.SerializeToNode(item, options.SerializerOptions);
                var payload = JsonSerializer.Serialize(new { param = paramName, value }, options.SerializerOptions);
                var evt = new RunEvent
                {
                    RunId = runId,
                    EventType = RunEventType.Input,
                    Payload = payload,
                    CreatedAt = timeProvider.GetUtcNow()
                };
                await store.AppendEventAsync(evt, ct);
                await notifications.PublishAsync(NotificationChannels.RunInput(runId), "", ct);
            }

            var completePayload = JsonSerializer.Serialize(new { param = paramName }, options.SerializerOptions);
            var completeEvt = new RunEvent
            {
                RunId = runId,
                EventType = RunEventType.InputComplete,
                Payload = completePayload,
                CreatedAt = timeProvider.GetUtcNow()
            };
            await store.AppendEventAsync(completeEvt);
            await notifications.PublishAsync(NotificationChannels.RunInput(runId), "");
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            // Source stream was cancelled externally (not our pump token) — signal cancellation
            // so the consumer is treated as cancelled rather than silently completing with partial data.
            try
            {
                var cancelPayload = JsonSerializer.Serialize(new { param = paramName, cancelled = true }, options.SerializerOptions);
                var cancelEvt = new RunEvent
                {
                    RunId = runId,
                    EventType = RunEventType.InputComplete,
                    Payload = cancelPayload,
                    CreatedAt = timeProvider.GetUtcNow()
                };
                await store.AppendEventAsync(cancelEvt);
                await notifications.PublishAsync(NotificationChannels.RunInput(runId), "");
            }
            catch { /* best-effort */ }
        }
        catch (OperationCanceledException)
        {
            // Our pump token was cancelled (parent cleanup or shutdown) — clean end of input
            try
            {
                var payload = JsonSerializer.Serialize(new { param = paramName }, options.SerializerOptions);
                var evt = new RunEvent
                {
                    RunId = runId,
                    EventType = RunEventType.InputComplete,
                    Payload = payload,
                    CreatedAt = timeProvider.GetUtcNow()
                };
                await store.AppendEventAsync(evt);
                await notifications.PublishAsync(NotificationChannels.RunInput(runId), "");
            }
            catch { /* best-effort */ }
        }
        catch (Exception ex)
        {
            try
            {
                var errorPayload = JsonSerializer.Serialize(new { param = paramName, error = ex.ToString() }, options.SerializerOptions);
                var errorEvt = new RunEvent
                {
                    RunId = runId,
                    EventType = RunEventType.InputComplete,
                    Payload = errorPayload,
                    CreatedAt = timeProvider.GetUtcNow()
                };
                await store.AppendEventAsync(errorEvt);
                await notifications.PublishAsync(NotificationChannels.RunInput(runId), "");
            }
            catch { /* best-effort error reporting */ }
        }
    }

    private JobRun CreateRun(string jobName, string? argsJson, string? id = null, RunOptions? options = null)
    {
        var now = timeProvider.GetUtcNow();
        var jobDef = registry.Get(jobName)?.Definition;
        return new JobRun
        {
            Id = id ?? Guid.CreateVersion7().ToString("N"),
            JobName = jobName,
            Arguments = argsJson,
            CreatedAt = now,
            NotBefore = options?.NotBefore ?? now,
            Priority = options?.Priority ?? jobDef?.Priority ?? 0,
            ParentRunId = JobContext.Current.Value?.RunId
        };
    }
}
