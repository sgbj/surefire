using System.Diagnostics.CodeAnalysis;

namespace Surefire;

/// <summary>Client for triggering, running, and managing job runs.</summary>
/// <remarks>
///     <para>Public surface is organized along two axes:</para>
///     <list type="bullet">
///         <item>
///             <b>Trigger-and-own</b> (<c>Run*</c> / <c>Stream*</c>): schedule work and consume
///             its outcome. Cancelling the caller's <see cref="System.Threading.CancellationToken" />
///             cancels the triggered run/batch.
///         </item>
///         <item>
///             <b>Observe</b> (<c>Wait*</c> / <c>Observe*EventsAsync</c>): consume an existing
///             run or batch without owning it. Cancelling the caller's token only stops the observer.
///         </item>
///     </list>
/// </remarks>
public interface IJobClient
{
    // -----------------------------------------------------------------------
    // Trigger primitives
    // -----------------------------------------------------------------------

    /// <summary>Schedules a single run of <paramref name="job" /> and returns immediately.</summary>
    /// <param name="job">The registered job name.</param>
    /// <param name="args">Optional arguments for the run, bound to the handler's parameters.</param>
    /// <param name="options">
    ///     Optional run-level overrides such as <see cref="RunOptions.NotBefore" />, priority, and
    ///     deduplication.
    /// </param>
    /// <param name="cancellationToken">A token to cancel the trigger call.</param>
    /// <returns>The created run.</returns>
    Task<JobRun> TriggerAsync(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Schedules a heterogeneous batch of runs and returns immediately.</summary>
    /// <param name="runs">The batch items, each carrying its own job name, arguments, and options.</param>
    /// <param name="cancellationToken">A token to cancel the trigger call.</param>
    /// <returns>The created batch.</returns>
    Task<JobBatch> TriggerBatchAsync(IEnumerable<BatchItem> runs, CancellationToken cancellationToken = default);

    /// <summary>Schedules a homogeneous batch — one run of <paramref name="job" /> per element of <paramref name="args" />.</summary>
    Task<JobBatch> TriggerBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null,
        CancellationToken cancellationToken = default);

    // -----------------------------------------------------------------------
    // Query
    // -----------------------------------------------------------------------

    /// <summary>Returns the run with the specified ID, or <c>null</c> if not found.</summary>
    Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Enumerates runs matching <paramref name="filter" />, snapshotted at the start of enumeration so concurrent
    ///     inserts don't shift pagination.
    /// </summary>
    IAsyncEnumerable<JobRun> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default);

    /// <summary>Returns the batch with the specified ID, or <c>null</c> if not found.</summary>
    Task<JobBatch?> GetBatchAsync(string batchId, CancellationToken cancellationToken = default);

    // -----------------------------------------------------------------------
    // Control
    // -----------------------------------------------------------------------

    /// <summary>Cancels a run and all of its descendants.</summary>
    /// <exception cref="RunNotFoundException">Thrown when the run does not exist.</exception>
    Task CancelAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>Cancels every non-terminal run in a batch and marks the batch terminal.</summary>
    Task CancelBatchAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>Creates a new run that re-executes <paramref name="runId" /> with the same arguments and input streams.</summary>
    /// <exception cref="RunNotFoundException">Thrown when the source run does not exist.</exception>
    Task<JobRun> RerunAsync(string runId, CancellationToken cancellationToken = default);

    // -----------------------------------------------------------------------
    // Observation primitives — raw events, no hydration, no interpretation
    // -----------------------------------------------------------------------

    /// <summary>
    ///     Yields every <see cref="RunEvent" /> for <paramref name="runId" /> with <c>Id &gt; sinceEventId</c>,
    ///     across all attempts and all event types, until the run reaches a terminal status.
    /// </summary>
    /// <remarks>
    ///     This primitive does not filter, interpret, or throw on non-success terminals. Consumers needing
    ///     retry-aware, attempt-scoped, or error-sensitive behavior build it on top of this stream.
    /// </remarks>
    /// <param name="runId">The run identifier.</param>
    /// <param name="sinceEventId">Exclusive lower bound on event IDs. Use <c>0</c> to start from the beginning.</param>
    /// <param name="cancellationToken">A token to cancel streaming. Cancellation does not affect the run itself.</param>
    /// <exception cref="RunNotFoundException">Thrown when the run does not exist.</exception>
    IAsyncEnumerable<RunEvent> ObserveRunEventsAsync(string runId, long sinceEventId = 0,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Yields every <see cref="RunEvent" /> produced by any direct child of <paramref name="batchId" />
    ///     with <c>Id &gt; sinceEventId</c>, in commit order, until the batch reaches a terminal status.
    /// </summary>
    /// <remarks>
    ///     Identical contract to <see cref="ObserveRunEventsAsync" /> but over a batch's children — raw events,
    ///     no filtering, no interpretation, no exceptions on individual child non-success terminals.
    /// </remarks>
    /// <param name="batchId">The batch identifier.</param>
    /// <param name="sinceEventId">Exclusive lower bound on event IDs. Use <c>0</c> to start from the beginning.</param>
    /// <param name="cancellationToken">A token to cancel streaming. Cancellation does not affect the batch itself.</param>
    IAsyncEnumerable<RunEvent> ObserveBatchEventsAsync(string batchId, long sinceEventId = 0,
        CancellationToken cancellationToken = default);

    // -----------------------------------------------------------------------
    // Single-run observation (Wait*: observe an existing run, never owns it)
    // -----------------------------------------------------------------------

    /// <summary>
    ///     Waits until the specified run reaches a terminal status and returns the final <see cref="JobRun" />
    ///     snapshot — regardless of success, failure, or cancellation. Non-throwing metadata API.
    /// </summary>
    /// <param name="runId">The run identifier.</param>
    /// <param name="cancellationToken">A token to cancel waiting. Cancellation does not affect the run itself.</param>
    /// <exception cref="RunNotFoundException">Thrown when the run does not exist.</exception>
    Task<JobRun> WaitAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Waits until the specified run reaches a terminal status and returns its deserialized result.
    /// </summary>
    /// <typeparam name="T">
    ///     The expected result shape. May be a scalar, collection (<c>List&lt;U&gt;</c>, <c>U[]</c>,
    ///     <c>IReadOnlyList&lt;U&gt;</c>), or <c>IAsyncEnumerable&lt;U&gt;</c>.
    /// </typeparam>
    /// <exception cref="JobRunException">Thrown on non-success terminal; inspect <see cref="JobRunException.Status" />.</exception>
    /// <exception cref="InvalidOperationException">
    ///     Thrown when the run has no result (null result column and no
    ///     <see cref="RunEventType.OutputComplete" /> event).
    /// </exception>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    Task<T> WaitAsync<T>(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Streams the deserialized output items of a running or terminal run.
    ///     Yields all Output events in commit order across every attempt (duplicates on retry are intentional —
    ///     use <see cref="WaitAsync{T}" /> with a collection <c>T</c> for the winning-attempt-only view).
    /// </summary>
    /// <param name="runId">The run identifier.</param>
    /// <param name="cancellationToken">A token to cancel streaming. Cancellation does not affect the run itself.</param>
    /// <exception cref="JobRunException">Thrown on next <c>MoveNextAsync</c> when the run's final terminal is non-success.</exception>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, CancellationToken cancellationToken = default);

    // -----------------------------------------------------------------------
    // Single-run trigger-and-own (Run* / Stream*: schedule and consume)
    // -----------------------------------------------------------------------

    /// <summary>Triggers <paramref name="job" /> and waits for it to complete, returning the deserialized result.</summary>
    /// <exception cref="JobRunException" />
    /// <exception cref="InvalidOperationException">Thrown when the run has no result.</exception>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    Task<T> RunAsync<T>(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Triggers <paramref name="job" /> and waits for it to complete.</summary>
    /// <exception cref="JobRunException">Thrown when the run ended non-successfully.</exception>
    Task RunAsync(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Triggers <paramref name="job" /> and streams its output items as they're produced.</summary>
    /// <remarks>
    ///     Cancellation of <paramref name="cancellationToken" /> cancels the triggered run; use
    ///     <see cref="WaitStreamAsync{T}" /> to stream an existing run without owning it.
    /// </remarks>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    IAsyncEnumerable<T> StreamAsync<T>(string job, object? args = null, RunOptions? options = null,
        CancellationToken cancellationToken = default);

    // -----------------------------------------------------------------------
    // Batch observation (Wait*: observe an existing batch, never owns it)
    // -----------------------------------------------------------------------

    /// <summary>
    ///     Waits until every child of <paramref name="batchId" /> reaches a terminal status and returns the
    ///     final <see cref="JobBatch" /> snapshot — regardless of individual child outcomes. Non-throwing metadata API.
    /// </summary>
    Task<JobBatch> WaitBatchAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Waits for every child to terminate and returns their hydrated results in terminal-commit order.
    ///     If any child failed or was cancelled, throws <see cref="AggregateException" /> at the end
    ///     (collect-all semantics).
    /// </summary>
    /// <exception cref="AggregateException">Thrown at the end when one or more children failed or were cancelled.</exception>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    Task<IReadOnlyList<T>> WaitBatchAsync<T>(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Yields each child run of <paramref name="batchId" /> as it reaches a terminal status, in
    ///     commit order — <b>regardless of individual child outcomes</b>. Non-throwing metadata stream.
    /// </summary>
    IAsyncEnumerable<JobRun> WaitEachAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Yields each child's hydrated result as it becomes terminal, in commit order. Fail-fast: on the
    ///     first non-success terminal observed, throws <see cref="JobRunException" /> at the yield position.
    ///     The batch itself keeps running.
    /// </summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    IAsyncEnumerable<T> WaitEachAsync<T>(string batchId, CancellationToken cancellationToken = default);

    // -----------------------------------------------------------------------
    // Batch trigger-and-own (Run* / Stream*: schedule and consume)
    // -----------------------------------------------------------------------

    /// <summary>Triggers a homogeneous batch and waits for every child to complete, returning their hydrated results.</summary>
    /// <exception cref="AggregateException">Thrown at the end when one or more children failed or were cancelled.</exception>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    Task<IReadOnlyList<T>> RunBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Triggers a homogeneous batch and waits for every child to complete.</summary>
    /// <exception cref="AggregateException">Thrown at the end when one or more children failed or were cancelled.</exception>
    Task RunBatchAsync(string job, IEnumerable<object?> args, RunOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>Triggers a heterogeneous batch and waits for every child to complete, returning their hydrated results.</summary>
    /// <exception cref="AggregateException">Thrown at the end when one or more children failed or were cancelled.</exception>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    Task<IReadOnlyList<T>> RunBatchAsync<T>(IEnumerable<BatchItem> items,
        CancellationToken cancellationToken = default);

    /// <summary>Triggers a heterogeneous batch and waits for every child to complete.</summary>
    /// <exception cref="AggregateException">Thrown at the end when one or more children failed or were cancelled.</exception>
    Task RunBatchAsync(IEnumerable<BatchItem> items, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a homogeneous batch and streams each child's hydrated result in terminal-commit order.
    ///     Cancellation of <paramref name="cancellationToken" /> cancels the batch.
    /// </summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    IAsyncEnumerable<T> StreamBatchAsync<T>(string job, IEnumerable<object?> args, RunOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a heterogeneous batch and streams each child's hydrated result in terminal-commit order.
    ///     Cancellation of <paramref name="cancellationToken" /> cancels the batch.
    /// </summary>
    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    IAsyncEnumerable<T> StreamBatchAsync<T>(IEnumerable<BatchItem> items,
        CancellationToken cancellationToken = default);
}