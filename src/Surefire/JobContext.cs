using System.Collections.Concurrent;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Surefire;

/// <summary>
///     Provides context for a job execution, including run metadata, cancellation, and progress reporting.
/// </summary>
public sealed class JobContext
{
    private static readonly AsyncLocal<JobContext?> CurrentContext = new();

    // Leading + trailing throttle. Terminal transitions await _inFlightTrailingFlush so progress
    // events never land after the terminal status.
    private static readonly TimeSpan MinProgressInterval = TimeSpan.FromMilliseconds(100);
    private readonly Lock _progressGate = new();
    private bool _hasReportedProgress;
    private Task? _inFlightTrailingFlush;
    private long _lastFlushedTicksUtc;

    private int _nextStep;
    private ITimer? _pendingTimer;
    private double? _pendingValue;

    internal static JobContext? Current => CurrentContext.Value;

    /// <summary>
    ///     Gets the unique identifier of the current run.
    /// </summary>
    public required string RunId { get; init; }

    /// <summary>
    ///     Gets the run ID of the root ancestor in the current execution hierarchy.
    /// </summary>
    public required string RootRunId { get; init; }

    /// <summary>
    ///     Gets the name of the job being executed.
    /// </summary>
    public required string JobName { get; init; }

    /// <summary>
    ///     Gets the cancellation token that is triggered when the run is Canceled or the node is shutting down.
    /// </summary>
    public required CancellationToken CancellationToken { get; init; }

    /// <summary>
    ///     Gets the failure-aware attempt number, counted from 1. For durable jobs, suspend / resume
    ///     cycles do not increment this counter.
    /// </summary>
    public int Attempt { get; init; }

    /// <summary>
    ///     Gets whether this durable orchestrator is still executing inside previously recorded
    ///     history. Always <c>false</c> for non-durable jobs. Use this for replay-safe logs and
    ///     metrics; use durable APIs such as <see cref="RecordAsync{T}" /> for non-deterministic
    ///     values or external side effects that must participate in replay.
    /// </summary>
    public bool IsReplaying => OrchestratorRunId is { } && Volatile.Read(ref _nextStep) < HighestRecordedStep;

    /// <summary>
    ///     Gets the batch ID if this run is part of a batch.
    /// </summary>
    public string? BatchId { get; init; }

    /// <summary>
    ///     Gets or sets the result produced by the job handler. Populated before lifecycle callbacks.
    /// </summary>
    public object? Result { get; internal set; }

    /// <summary>
    ///     Gets or sets the exception thrown by the job handler. Populated before lifecycle callbacks.
    /// </summary>
    public Exception? Exception { get; internal set; }

    /// <summary>
    ///     Gets a thread-safe key-value bag for passing data between filters in the pipeline.
    /// </summary>
    public IDictionary<string, object?> Items { get; } = new ConcurrentDictionary<string, object?>();

    internal IJobStore Store { get; init; } = null!;

    internal INotificationProvider Notifications { get; init; } = null!;

    internal BatchedEventWriter EventWriter { get; init; } = null!;

    internal TimeProvider TimeProvider { get; init; } = null!;

    internal JsonSerializerOptions SerializerOptions { get; init; } = null!;

    internal string NodeName { get; init; } = null!;

    /// <summary>
    ///     Gets whether the current run is a durable orchestrator. Use this together with
    ///     <see cref="IsReplaying" /> to distinguish non-durable code, durable-but-new-territory,
    ///     and durable-but-replaying.
    /// </summary>
    public bool IsDurable => OrchestratorRunId is { };

    /// <summary>
    ///     When non-null, identifies the durable orchestrator owning this execution. Used by
    ///     <see cref="IJobClient" /> to derive deterministic child run / batch ids via
    ///     <c>DurableIds.DerivedRunId(OrchestratorRunId, step)</c>.
    /// </summary>
    internal string? OrchestratorRunId { get; init; }

    /// <summary>
    ///     The highest step number recorded by prior executions of this orchestrator, captured
    ///     once at claim time. Frozen for the duration of this execution. The handler is in
    ///     "recorded replay" territory while <c>_nextStep &lt; HighestRecordedStep</c> and in
    ///     "new" territory thereafter; see <see cref="IsReplaying" />.
    /// </summary>
    internal int HighestRecordedStep { get; init; }

    /// <summary>
    ///     Bulk-loaded snapshot of the orchestrator's recorded children and on-row replay counters,
    ///     captured once at claim time. Used by <see cref="IJobClient" /> replay paths so every
    ///     <c>RunAsync</c> / <c>RunBatchAsync</c> call satisfies itself from in-memory state
    ///     rather than per-call store reads. Null for non-durable runs.
    /// </summary>
    internal DurableExecutionSnapshot? DurableSnapshot { get; init; }

    /// <summary>
    ///     Collector for the run ids / batch ids the handler awaited during this invocation.
    ///     Both throw-yield iterator methods and cooperative-TCS await methods write here; the
    ///     executor reads here once when deciding whether to suspend. Null for non-durable runs.
    /// </summary>
    internal PendingAwaitSet? PendingAwaits { get; init; }

    /// <summary>
    ///     Allocates a step index and captures the replay flag in the same call so concurrent
    ///     <see cref="IJobClient" /> calls inside a <c>Task.WhenAll</c> never observe one another's
    ///     step increments through the shared <see cref="IsReplaying" /> read.
    /// </summary>
    internal (int Step, bool IsReplay) AllocateStep()
    {
        var step = Interlocked.Increment(ref _nextStep);
        return (step, HasRecordedOperation(step));
    }

    /// <summary>
    ///     Records a value produced by <paramref name="valueFactory" /> so durable replay returns
    ///     the same value without invoking the factory again.
    /// </summary>
    /// <typeparam name="T">The recorded value type. Keep this type stable for running orchestrators.</typeparam>
    /// <param name="name">A non-empty diagnostic name for the recorded value. It does not need to be unique.</param>
    /// <param name="valueFactory">The factory invoked only when recording new durable history.</param>
    /// <param name="cancellationToken">
    ///     A token to cancel the store write. When omitted, the job cancellation token is used.
    /// </param>
    /// <returns>The newly recorded value, or the stored value during replay.</returns>
    /// <exception cref="InvalidOperationException">Thrown when called outside a durable orchestrator.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="name" /> is empty.</exception>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="valueFactory" /> is null.</exception>
    /// <exception cref="DurableReplayMismatchException">Thrown when code no longer matches replay history.</exception>
    public ValueTask<T> RecordAsync<T>(string name, Func<ValueTask<T>> valueFactory,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(valueFactory);

        // Resolve T through the configured serializer options, the same AOT-safe path job
        // arguments and results use.
        var jsonTypeInfo = (JsonTypeInfo<T>)SerializerOptions.GetTypeInfo(typeof(T));
        return RecordValueAsync(DurableRecordKinds.Record, name, valueFactory, jsonTypeInfo, cancellationToken);
    }

    /// <summary>Records and returns a replay-safe equivalent of <see cref="Guid.NewGuid" />.</summary>
    public ValueTask<Guid> NewGuidAsync(CancellationToken cancellationToken = default) =>
        RecordValueAsync(DurableRecordKinds.GuidV4, null, () => new ValueTask<Guid>(Guid.NewGuid()),
            SurefireJsonContext.Default.Guid, cancellationToken);

    /// <summary>Records and returns a replay-safe version 7 GUID.</summary>
    public ValueTask<Guid> NewGuidV7Async(CancellationToken cancellationToken = default) =>
        RecordValueAsync(DurableRecordKinds.GuidV7, null,
            () => new ValueTask<Guid>(Guid.CreateVersion7(TimeProvider.GetUtcNow())),
            SurefireJsonContext.Default.Guid, cancellationToken);

    /// <summary>Records and returns a replay-safe equivalent of <see cref="TimeProvider.GetUtcNow" />.</summary>
    public ValueTask<DateTimeOffset> GetUtcNowAsync(CancellationToken cancellationToken = default) =>
        RecordValueAsync(DurableRecordKinds.UtcNow, null, () => new ValueTask<DateTimeOffset>(TimeProvider.GetUtcNow()),
            SurefireJsonContext.Default.DateTimeOffset, cancellationToken);

    /// <summary>Records and returns a replay-safe non-negative random integer.</summary>
    public ValueTask<int> NextInt32Async(CancellationToken cancellationToken = default) =>
        RecordRandomInt32Async(() => Random.Shared.Next(), null, null, cancellationToken);

    /// <summary>Records and returns a replay-safe random integer less than <paramref name="maxValue" />.</summary>
    public ValueTask<int> NextInt32Async(int maxValue, CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(maxValue);
        return RecordRandomInt32Async(() => Random.Shared.Next(maxValue), 0, maxValue, cancellationToken);
    }

    /// <summary>
    ///     Records and returns a replay-safe random integer in the half-open range
    ///     <paramref name="minValue" /> to <paramref name="maxValue" />.
    /// </summary>
    public ValueTask<int> NextInt32Async(int minValue, int maxValue, CancellationToken cancellationToken = default)
    {
        if (minValue > maxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(minValue), "minValue must be less than or equal to maxValue.");
        }

        return RecordRandomInt32Async(() => Random.Shared.Next(minValue, maxValue), minValue, maxValue,
            cancellationToken);
    }

    /// <summary>Records and returns a replay-safe random double greater than or equal to 0.0 and less than 1.0.</summary>
    public ValueTask<double> NextDoubleAsync(CancellationToken cancellationToken = default) =>
        RecordValueAsync(DurableRecordKinds.RandomDouble, null, () => new ValueTask<double>(Random.Shared.NextDouble()),
            SurefireJsonContext.Default.Double, cancellationToken);

    internal void ValidateReplayChildRun(int step, string runId)
    {
        var operation = GetRecordedOperation(step, out var record, out var recordedId);
        if (operation != DurableRecordedOperation.ChildRun ||
            !string.Equals(recordedId, runId, StringComparison.Ordinal))
        {
            throw BuildReplayMismatch(step, DescribeOperation(DurableRecordedOperation.ChildRun, runId),
                DescribeOperation(operation, recordedId, record));
        }
    }

    internal void ValidateReplayChildBatch(int step, string batchId)
    {
        var operation = GetRecordedOperation(step, out var record, out var recordedId);
        if (operation != DurableRecordedOperation.ChildBatch ||
            !string.Equals(recordedId, batchId, StringComparison.Ordinal))
        {
            throw BuildReplayMismatch(step, DescribeOperation(DurableRecordedOperation.ChildBatch, batchId),
                DescribeOperation(operation, recordedId, record));
        }
    }

    internal void ThrowIfRecordedStepsSkipped()
    {
        if (OrchestratorRunId is null)
        {
            return;
        }

        var currentStep = Volatile.Read(ref _nextStep);
        if (currentStep < HighestRecordedStep)
        {
            throw BuildReplayMismatch(currentStep + 1,
                "the next recorded durable operation",
                "handler completed before replaying all recorded durable steps");
        }
    }

    internal static IDisposable EnterScope(JobContext context)
    {
        var previous = CurrentContext.Value;
        CurrentContext.Value = context;
        return new Scope(previous);
    }

    private async ValueTask<T> RecordValueAsync<T>(string kind, string? name, Func<ValueTask<T>> valueFactory,
        JsonTypeInfo<T> jsonTypeInfo, CancellationToken cancellationToken)
    {
        var effectiveToken = cancellationToken.CanBeCanceled ? cancellationToken : CancellationToken;
        var (step, isReplay) = AllocateDurableStep();
        if (isReplay)
        {
            var replayRecord = GetReplayRecord(step, kind, name);
            return JsonSerializer.Deserialize(replayRecord.Payload, jsonTypeInfo)!;
        }

        var value = await valueFactory();
        var payload = JsonSerializer.Serialize(value, jsonTypeInfo);
        var record = new DurableRecord(
            OrchestratorRunId!,
            step,
            kind,
            name,
            payload,
            TimeProvider.GetUtcNow());
        await Store.CreateDurableRecordAsync(record, effectiveToken);
        return value;
    }

    private async ValueTask<int> RecordRandomInt32Async(Func<int> valueFactory, int? minValue, int? maxValue,
        CancellationToken cancellationToken)
    {
        var effectiveToken = cancellationToken.CanBeCanceled ? cancellationToken : CancellationToken;
        var (step, isReplay) = AllocateDurableStep();
        if (isReplay)
        {
            var replayRecord = GetReplayRecord(step, DurableRecordKinds.RandomInt32, null);
            var replayPayload = JsonSerializer.Deserialize(replayRecord.Payload,
                                    SurefireJsonContext.Default.DurableRandomInt32Payload)
                                ?? throw BuildReplayMismatch(step,
                                    DescribeRandomInt32(minValue, maxValue),
                                    "null random-int32 payload");
            if (replayPayload.MinValue != minValue || replayPayload.MaxValue != maxValue)
            {
                throw BuildReplayMismatch(step,
                    DescribeRandomInt32(minValue, maxValue),
                    DescribeRandomInt32(replayPayload.MinValue, replayPayload.MaxValue));
            }

            if (!IsValidRandomInt32Value(replayPayload.Value, minValue, maxValue))
            {
                throw BuildReplayMismatch(step,
                    $"value in {DescribeRandomInt32(minValue, maxValue)}",
                    replayPayload.Value.ToString(CultureInfo.InvariantCulture));
            }

            return replayPayload.Value;
        }

        var value = valueFactory();
        var payload = JsonSerializer.Serialize(
            new()
            {
                Value = value,
                MinValue = minValue,
                MaxValue = maxValue
            },
            SurefireJsonContext.Default.DurableRandomInt32Payload);
        var record = new DurableRecord(
            OrchestratorRunId!,
            step,
            DurableRecordKinds.RandomInt32,
            null,
            payload,
            TimeProvider.GetUtcNow());
        await Store.CreateDurableRecordAsync(record, effectiveToken);
        return value;
    }

    private (int Step, bool IsReplay) AllocateDurableStep()
    {
        if (OrchestratorRunId is null)
        {
            throw new InvalidOperationException("Recorded durable values can only be used inside a durable job.");
        }

        return AllocateStep();
    }

    private bool HasRecordedOperation(int step)
    {
        if (OrchestratorRunId is not { } orchestratorRunId || DurableSnapshot is null)
        {
            return false;
        }

        if (DurableSnapshot.Records.ContainsKey(step))
        {
            return true;
        }

        return DurableSnapshot.Children.ContainsKey(DurableIds.DerivedRunId(orchestratorRunId, step))
               || DurableSnapshot.ChildBatches.ContainsKey(DurableIds.DerivedBatchId(orchestratorRunId, step));
    }

    private DurableRecord GetReplayRecord(int step, string kind, string? name)
    {
        var operation = GetRecordedOperation(step, out var record, out var recordedId);
        if (operation != DurableRecordedOperation.Record || record is null)
        {
            throw BuildReplayMismatch(step, DescribeRecord(kind, name),
                DescribeOperation(operation, recordedId, record));
        }

        if (!string.Equals(record.Kind, kind, StringComparison.Ordinal)
            || !string.Equals(record.Name, name, StringComparison.Ordinal))
        {
            throw BuildReplayMismatch(step, DescribeRecord(kind, name), DescribeRecord(record.Kind, record.Name));
        }

        return record;
    }

    private DurableRecordedOperation GetRecordedOperation(int step, out DurableRecord? record, out string? recordedId)
    {
        if (OrchestratorRunId is not { } orchestratorRunId || DurableSnapshot is null)
        {
            throw new InvalidOperationException("Durable replay history is not available for the current job.");
        }

        record = null;
        recordedId = null;
        DurableRecordedOperation? operation = null;

        if (DurableSnapshot.Records.TryGetValue(step, out var durableRecord))
        {
            record = durableRecord;
            operation = DurableRecordedOperation.Record;
        }

        var childRunId = DurableIds.DerivedRunId(orchestratorRunId, step);
        if (DurableSnapshot.Children.ContainsKey(childRunId))
        {
            EnsureSingleRecordedOperation(step, operation, DurableRecordedOperation.ChildRun);
            recordedId = childRunId;
            operation = DurableRecordedOperation.ChildRun;
        }

        var childBatchId = DurableIds.DerivedBatchId(orchestratorRunId, step);
        if (DurableSnapshot.ChildBatches.ContainsKey(childBatchId))
        {
            EnsureSingleRecordedOperation(step, operation, DurableRecordedOperation.ChildBatch);
            recordedId = childBatchId;
            operation = DurableRecordedOperation.ChildBatch;
        }

        return operation ?? throw BuildReplayMismatch(step, "recorded durable operation", "no recorded operation");
    }

    private void EnsureSingleRecordedOperation(int step, DurableRecordedOperation? existing,
        DurableRecordedOperation next)
    {
        if (existing is { } prior)
        {
            throw BuildReplayMismatch(step, DescribeOperation(next, null),
                $"ambiguous history containing both {prior} and {next}");
        }
    }

    private DurableReplayMismatchException BuildReplayMismatch(int step, string expected, string actual) =>
        new(OrchestratorRunId ?? RunId, step, $"Expected {expected}; saw {actual}.");

    private static string DescribeOperation(DurableRecordedOperation operation, string? recordedId,
        DurableRecord? record = null) => operation switch
    {
        DurableRecordedOperation.ChildRun => $"child run '{recordedId}'",
        DurableRecordedOperation.ChildBatch => $"child batch '{recordedId}'",
        DurableRecordedOperation.Record when record is { } r => DescribeRecord(r.Kind, r.Name),
        DurableRecordedOperation.Record => "recorded value",
        _ => operation.ToString()
    };

    private static string DescribeRecord(string kind, string? name) =>
        name is { Length: > 0 }
            ? $"record '{name}' ({kind})"
            : $"record kind '{kind}'";

    private static string DescribeRandomInt32(int? minValue, int? maxValue) =>
        (minValue, maxValue) switch
        {
            (null, null) => "random-int32 with no bounds",
            (0, { } max) => $"random-int32 with maxValue {max.ToString(CultureInfo.InvariantCulture)}",
            ({ } min, { } max) =>
                $"random-int32 with range {min.ToString(CultureInfo.InvariantCulture)}..{max.ToString(CultureInfo.InvariantCulture)}",
            _ => "random-int32 with invalid bounds"
        };

    private static bool IsValidRandomInt32Value(int value, int? minValue, int? maxValue)
    {
        if (minValue is null && maxValue is null)
        {
            return value >= 0 && value < int.MaxValue;
        }

        var min = minValue.GetValueOrDefault();
        var max = maxValue.GetValueOrDefault();
        return min == max
            ? value == min
            : value >= min && value < max;
    }

    /// <summary>
    ///     Reports execution progress to the store and connected clients. Successive calls within a
    ///     100 ms window are coalesced: the first is flushed immediately, and the last one in the
    ///     window is flushed at the window's end. The terminal value is always persisted.
    /// </summary>
    /// <param name="progress">A value between 0.0 and 1.0 inclusive.</param>
    /// <returns>
    ///     A task that completes when the progress has been persisted for leading-edge reports, or
    ///     immediately when the report was coalesced into a pending trailing flush.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException">
    ///     Thrown when <paramref name="progress" /> is less than 0.0 or greater than 1.0.
    /// </exception>
    public Task ReportProgressAsync(double progress)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(progress, 0.0);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(progress, 1.0);

        lock (_progressGate)
        {
            var nowTicks = TimeProvider.GetUtcNow().UtcTicks;
            var elapsedTicks = nowTicks - _lastFlushedTicksUtc;
            var leading = !_hasReportedProgress || elapsedTicks >= MinProgressInterval.Ticks;

            if (leading)
            {
                _hasReportedProgress = true;
                _lastFlushedTicksUtc = nowTicks;
                _pendingValue = null;
                DisposePendingTimer();
                return PersistProgressAsync(progress);
            }

            _pendingValue = progress;
            if (_pendingTimer is null)
            {
                var delay = TimeSpan.FromTicks(MinProgressInterval.Ticks - elapsedTicks);
                _pendingTimer = TimeProvider.CreateTimer(FlushPendingCallback, null, delay, Timeout.InfiniteTimeSpan);
            }

            return Task.CompletedTask;
        }
    }

    /// <summary>
    ///     Flushes any pending trailing-edge progress value, awaiting any flush already started by
    ///     the timer callback. Called by the executor before transitioning the run to a terminal
    ///     status so the last coalesced value lands and never arrives after the terminal status.
    /// </summary>
    internal async Task FlushPendingProgressAsync(CancellationToken cancellationToken)
    {
        Task? inFlight;
        double? value;
        lock (_progressGate)
        {
            // Holding the lock guarantees a concurrent timer callback has either already
            // published its task to _inFlightTrailingFlush, or will find _pendingValue null
            // and return without persisting.
            inFlight = _inFlightTrailingFlush;
            value = _pendingValue;
            _pendingValue = null;
            DisposePendingTimer();
            if (value is { })
            {
                _lastFlushedTicksUtc = TimeProvider.GetUtcNow().UtcTicks;
            }
        }

        if (inFlight is { })
        {
            try
            {
                await inFlight;
            }
            catch (OperationCanceledException)
            {
            }
        }

        if (value is { } v)
        {
            await PersistProgressAsync(v, cancellationToken);
        }
    }

    private void FlushPendingCallback(object? _)
    {
        lock (_progressGate)
        {
            if (_pendingValue is not { } v)
            {
                return;
            }

            _pendingValue = null;
            _lastFlushedTicksUtc = TimeProvider.GetUtcNow().UtcTicks;
            DisposePendingTimer();
            // Publish under lock so FlushPendingProgressAsync never observes a running persist
            // with a null field.
            _inFlightTrailingFlush = PersistProgressAsync(v);
        }
    }

    private void DisposePendingTimer()
    {
        _pendingTimer?.Dispose();
        _pendingTimer = null;
    }

    private Task PersistProgressAsync(double progress) => PersistProgressAsync(progress, CancellationToken);

    private async Task PersistProgressAsync(double progress, CancellationToken cancellationToken)
    {
        var now = TimeProvider.GetUtcNow();
        await Store.UpdateRunAsync(new()
        {
            Id = RunId,
            JobName = JobName,
            NodeName = NodeName,
            Progress = progress,
            LastHeartbeatAt = now
        }, cancellationToken);

        await EventWriter.EnqueueAsync(
            new()
            {
                RunId = RunId,
                EventType = RunEventType.Progress,
                Payload = progress.ToString(CultureInfo.InvariantCulture),
                CreatedAt = now,
                Attempt = Attempt
            },
            [new(NotificationChannels.RunEvent(RunId), RunId)],
            cancellationToken);
    }

    private sealed class Scope(JobContext? previous) : IDisposable
    {
        public void Dispose() => CurrentContext.Value = previous;
    }
}
