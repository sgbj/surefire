namespace Surefire;

/// <summary>
///     The primary API for triggering, running, streaming, and managing jobs.
///     Higher-level methods compose from lower-level ones: <c>RunAsync</c> = <c>TriggerAsync</c> + <c>WaitAsync</c>,
///     <c>RunAllAsync</c> = <c>RunEachAsync</c> materialized to array, etc.
/// </summary>
public interface IJobClient
{
    /// <summary>
    ///     Creates a pending run for the specified job and returns immediately.
    /// </summary>
    /// <param name="jobName">The name of the job to trigger.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The ID of the created run.</returns>
    Task<string> TriggerAsync(string jobName, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a pending run for the specified job with arguments and returns immediately.
    /// </summary>
    /// <param name="jobName">The name of the job to trigger.</param>
    /// <param name="args">The arguments to pass to the job handler.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The ID of the created run.</returns>
    Task<string> TriggerAsync(string jobName, object? args, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a pending run for the specified job with arguments and scheduling options,
    ///     and returns immediately.
    /// </summary>
    /// <param name="jobName">The name of the job to trigger.</param>
    /// <param name="args">The arguments to pass to the job handler.</param>
    /// <param name="options">Options controlling scheduling, priority, and deduplication.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The ID of the created run.</returns>
    Task<string> TriggerAsync(string jobName, object? args, RunOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job and waits for it to reach a terminal status. Never throws for job failure --
    ///     inspect <see cref="RunResult.IsSuccess" /> and <see cref="RunResult.IsFailure" />.
    ///     Terminal statuses are <see cref="JobStatus.Completed" />, <see cref="JobStatus.Cancelled" />,
    ///     and <see cref="JobStatus.DeadLetter" />.
    ///     If <paramref name="cancellationToken" /> is cancelled while waiting, the created run is
    ///     also cancelled.
    /// </summary>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The result of the completed run.</returns>
    Task<RunResult> RunAsync(string jobName, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job with arguments and waits for it to reach a terminal status.
    ///     If <paramref name="cancellationToken" /> is cancelled while waiting, the created run is
    ///     also cancelled.
    /// </summary>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="args">The arguments to pass to the job handler.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The result of the completed run.</returns>
    Task<RunResult> RunAsync(string jobName, object? args, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job with arguments and options, and waits for it to reach a terminal status.
    ///     If <paramref name="cancellationToken" /> is cancelled while waiting, the created run is
    ///     also cancelled.
    /// </summary>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="args">The arguments to pass to the job handler.</param>
    /// <param name="options">Options controlling scheduling, priority, and deduplication.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The result of the completed run.</returns>
    Task<RunResult> RunAsync(string jobName, object? args, RunOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job and waits for it to produce a typed result. Throws
    ///     <c>JobRunFailedException</c> if the run fails or is dead-lettered.
    ///     If <paramref name="cancellationToken" /> is cancelled while waiting, the created run is
    ///     also cancelled.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The deserialized result.</returns>
    Task<T> RunAsync<T>(string jobName, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job with arguments and waits for it to produce a typed result.
    ///     Throws <c>JobRunFailedException</c> if the run fails or is dead-lettered.
    ///     If <paramref name="cancellationToken" /> is cancelled while waiting, the created run is
    ///     also cancelled.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="args">The arguments to pass to the job handler.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The deserialized result.</returns>
    Task<T> RunAsync<T>(string jobName, object? args, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job with arguments and options, and waits for it to produce a typed result.
    ///     Throws <c>JobRunFailedException</c> if the run fails or is dead-lettered.
    ///     If <paramref name="cancellationToken" /> is cancelled while waiting, the created run is
    ///     also cancelled.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="args">The arguments to pass to the job handler.</param>
    /// <param name="options">Options controlling scheduling, priority, and deduplication.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The deserialized result.</returns>
    Task<T> RunAsync<T>(string jobName, object? args, RunOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job and streams its output values as they are produced. This is a convenience
    ///     alias for <c>RunAsync&lt;IAsyncEnumerable&lt;T&gt;&gt;</c>. Provides at-least-once delivery
    ///     of output items across retry attempts.
    /// </summary>
    /// <typeparam name="T">The type of items in the output stream.</typeparam>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable that yields output values as they are produced.</returns>
    IAsyncEnumerable<T> StreamAsync<T>(string jobName, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job with arguments and streams its output values as they are produced.
    /// </summary>
    /// <typeparam name="T">The type of items in the output stream.</typeparam>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="args">The arguments to pass to the job handler.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable that yields output values as they are produced.</returns>
    IAsyncEnumerable<T> StreamAsync<T>(string jobName, object? args, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a job with arguments and options, and streams its output values as they are produced.
    /// </summary>
    /// <typeparam name="T">The type of items in the output stream.</typeparam>
    /// <param name="jobName">The name of the job to run.</param>
    /// <param name="args">The arguments to pass to the job handler.</param>
    /// <param name="options">Options controlling scheduling, priority, and deduplication.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable that yields output values as they are produced.</returns>
    IAsyncEnumerable<T> StreamAsync<T>(string jobName, object? args, RunOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Observes a run as a stream of snapshots and events until the run reaches a terminal status.
    ///     This is the lowest-level run-consumption primitive used by higher-level wait and stream APIs.
    ///     Cancelling <paramref name="cancellationToken" /> cancels observation only.
    /// </summary>
    /// <param name="runId">The run ID to observe.</param>
    /// <param name="cancellationToken">A token to cancel observation.</param>
    /// <returns>An async enumerable of run observations.</returns>
    IAsyncEnumerable<RunObservation> ObserveAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Observes a run starting after the provided event cursor, enabling resume after disconnect.
    ///     Only events with IDs greater than <see cref="RunEventCursor.SinceEventId" /> are emitted.
    ///     Cancelling <paramref name="cancellationToken" /> cancels observation only.
    /// </summary>
    /// <param name="runId">The run ID to observe.</param>
    /// <param name="cursor">The event cursor to resume from.</param>
    /// <param name="cancellationToken">A token to cancel observation.</param>
    /// <returns>An async enumerable of run observations.</returns>
    IAsyncEnumerable<RunObservation> ObserveAsync(string runId, RunEventCursor cursor,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a batch of runs for the specified job (one per args entry) and returns the
    ///     batch coordinator run ID. All runs are inserted in a single atomic transaction.
    /// </summary>
    /// <param name="jobName">The name of the job to trigger for each item.</param>
    /// <param name="argsList">The arguments for each child run.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The batch coordinator run ID.</returns>
    Task<string> TriggerAllAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a batch of runs with scheduling options and returns the batch coordinator run ID.
    /// </summary>
    /// <param name="jobName">The name of the job to trigger for each item.</param>
    /// <param name="argsList">The arguments for each child run.</param>
    /// <param name="options">Options applied to the coordinator and propagated to children.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The batch coordinator run ID.</returns>
    Task<string> TriggerAllAsync(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a batch of runs and waits for all children to complete. Returns all results.
    ///     Never throws for individual child failures -- inspect each <see cref="RunResult" />.
    /// </summary>
    /// <param name="jobName">The name of the job to run for each item.</param>
    /// <param name="argsList">The arguments for each child run.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>An array of results, one per child, in completion order.</returns>
    Task<RunResult[]> RunAllAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a batch of runs with options and waits for all children to complete.
    /// </summary>
    /// <param name="jobName">The name of the job to run for each item.</param>
    /// <param name="argsList">The arguments for each child run.</param>
    /// <param name="options">Options applied to the coordinator and propagated to children.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>An array of results, one per child, in completion order.</returns>
    Task<RunResult[]> RunAllAsync(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a batch and waits for all children to produce typed results. Throws
    ///     <see cref="AggregateException" /> if any children fail.
    /// </summary>
    /// <typeparam name="T">The expected result type for each child.</typeparam>
    /// <param name="jobName">The name of the job to run for each item.</param>
    /// <param name="argsList">The arguments for each child run.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>An array of deserialized results, one per child.</returns>
    Task<T[]> RunAllAsync<T>(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a batch with options and waits for all children to produce typed results.
    ///     Throws <see cref="AggregateException" /> if any children fail.
    /// </summary>
    /// <typeparam name="T">The expected result type for each child.</typeparam>
    /// <param name="jobName">The name of the job to run for each item.</param>
    /// <param name="argsList">The arguments for each child run.</param>
    /// <param name="options">Options applied to the coordinator and propagated to children.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>An array of deserialized results, one per child.</returns>
    Task<T[]> RunAllAsync<T>(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a batch and yields each child's result as it completes. Results are yielded
    ///     in completion order, not creation order. Does not throw for individual child failures --
    ///     inspect each <see cref="RunResult" />.
    ///     If <paramref name="cancellationToken" /> is cancelled while waiting, the created batch
    ///     coordinator and non-terminal descendants are also cancelled.
    /// </summary>
    /// <param name="jobName">The name of the job to run for each item.</param>
    /// <param name="argsList">The arguments for each child run.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable of results yielded as children complete.</returns>
    IAsyncEnumerable<RunResult> RunEachAsync(string jobName, IEnumerable<object?> argsList,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Triggers a batch with options and yields each child's result as it completes.
    ///     If <paramref name="cancellationToken" /> is cancelled while waiting, the created batch
    ///     coordinator and non-terminal descendants are also cancelled.
    /// </summary>
    /// <param name="jobName">The name of the job to run for each item.</param>
    /// <param name="argsList">The arguments for each child run.</param>
    /// <param name="options">Options applied to the coordinator and propagated to children.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable of results yielded as children complete.</returns>
    IAsyncEnumerable<RunResult> RunEachAsync(string jobName, IEnumerable<object?> argsList, RunOptions options,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Waits for a run to reach a terminal status and returns its result. Transparently waits
    ///     through retries. Never throws for job failure -- inspect <see cref="RunResult.IsSuccess" />.
    ///     Cancelling <paramref name="cancellationToken" /> cancels the wait only; use
    ///     <see cref="CancelAsync" /> to cancel run execution.
    /// </summary>
    /// <param name="runId">The run ID to wait for.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The result of the completed run.</returns>
    Task<RunResult> WaitAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Waits for a run to reach terminal status and returns a typed result.
    ///     Throws <see cref="JobRunFailedException" /> when the run does not complete successfully.
    ///     Cancelling <paramref name="cancellationToken" /> cancels the wait only.
    /// </summary>
    /// <typeparam name="T">The expected result type.</typeparam>
    /// <param name="runId">The run ID to wait for.</param>
    /// <param name="cancellationToken">A token to cancel the wait.</param>
    /// <returns>The typed result value.</returns>
    Task<T> WaitAsync<T>(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Streams output values from an existing run until completion.
    ///     Cancelling <paramref name="cancellationToken" /> cancels the stream only.
    /// </summary>
    /// <typeparam name="T">The output item type.</typeparam>
    /// <param name="runId">The run ID to stream from.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable of output values.</returns>
    IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Streams output values from an existing run starting after the provided event cursor,
    ///     enabling resumable consumption.
    ///     Cancelling <paramref name="cancellationToken" /> cancels the stream only.
    /// </summary>
    /// <typeparam name="T">The output item type.</typeparam>
    /// <param name="runId">The run ID to stream from.</param>
    /// <param name="cursor">The event cursor to resume from.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable of output values.</returns>
    IAsyncEnumerable<T> WaitStreamAsync<T>(string runId, RunEventCursor cursor,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Streams output values from all children in a batch as events are produced.
    ///     Items are yielded in event order across all children.
    ///     Cancelling <paramref name="cancellationToken" /> cancels the stream only.
    /// </summary>
    /// <typeparam name="T">The output item type.</typeparam>
    /// <param name="batchId">The batch coordinator run ID.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable of child output items tagged with child run ID.</returns>
    IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Streams output values from all children in a batch starting after per-child cursor
    ///     checkpoints, enabling resumable batch output consumption.
    ///     Items are yielded in event order across all children.
    ///     Cancelling <paramref name="cancellationToken" /> cancels the stream only.
    /// </summary>
    /// <typeparam name="T">The output item type.</typeparam>
    /// <param name="batchId">The batch coordinator run ID.</param>
    /// <param name="cursor">Per-child event cursor checkpoints to resume from.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable of child output items tagged with child run ID.</returns>
    IAsyncEnumerable<BatchStreamItem<T>> StreamEachAsync<T>(string batchId, BatchRunEventCursor cursor,
        CancellationToken cancellationToken = default);

    /// <summary>
    ///     Yields each batch child's result as it reaches a terminal status. Results are yielded
    ///     in completion order. Cancelling <paramref name="cancellationToken" /> cancels the
    ///     stream only; use <see cref="CancelAsync" /> on the coordinator to cancel execution.
    /// </summary>
    /// <param name="batchId">The batch coordinator run ID.</param>
    /// <param name="cancellationToken">A token to cancel the stream.</param>
    /// <returns>An async enumerable of results yielded as children complete.</returns>
    IAsyncEnumerable<RunResult> WaitEachAsync(string batchId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Cancels a run. If the run is currently executing, its <see cref="CancellationToken" />
    ///     is signalled. If the run is a batch coordinator, all non-terminal descendants are
    ///     also cancelled.
    /// </summary>
    /// <param name="runId">The run ID to cancel.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    Task CancelAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Creates a new run that re-executes a previous run with the same job name and arguments.
    ///     The new run's <c>RerunOfRunId</c> points to the original. For batch coordinators,
    ///     all children are re-created.
    /// </summary>
    /// <param name="runId">The ID of the run to re-execute.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The ID of the newly created run.</returns>
    Task<string> RerunAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns the run with the specified ID, or null if not found.
    /// </summary>
    /// <param name="runId">The run ID.</param>
    /// <param name="cancellationToken">A token to cancel the operation.</param>
    /// <returns>The matching run, or null.</returns>
    Task<JobRun?> GetRunAsync(string runId, CancellationToken cancellationToken = default);

    /// <summary>
    ///     Returns all runs matching the specified filter, automatically paginating through the
    ///     store internally. All matching runs are yielded -- no hidden limits.
    /// </summary>
    /// <param name="filter">The filter criteria.</param>
    /// <param name="cancellationToken">A token to cancel the enumeration.</param>
    /// <returns>An async enumerable of all matching runs.</returns>
    IAsyncEnumerable<JobRun> GetRunsAsync(RunFilter filter, CancellationToken cancellationToken = default);
}