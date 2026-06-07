using System.Diagnostics;

namespace Surefire.Tests.Testing;

/// <summary>
///     Shared concurrency helpers used across the integration test suite.
/// </summary>
public static class TestConcurrency
{
    /// <summary>
    ///     Atomically updates <paramref name="location" /> to the greater of its current value
    ///     and <paramref name="candidate" />. Lock-free CAS loop suitable for tracking
    ///     high-water marks (max in-flight, max queue depth, etc.) from worker threads.
    /// </summary>
    public static void InterlockedMax(ref int location, int candidate)
    {
        int original;
        do
        {
            original = Volatile.Read(ref location);
            if (candidate <= original)
            {
                return;
            }
        } while (Interlocked.CompareExchange(ref location, candidate, original) != original);
    }

    /// <summary>
    ///     Polls <paramref name="predicate" /> until it returns true or the 20-second timeout
    ///     elapses. Throws <see cref="TimeoutException" /> on timeout. Intended for tests that
    ///     need to wait on store/runtime state changes without subscribing to notifications.
    /// </summary>
    public static async Task WaitForAsync(Func<Task<bool>> predicate, CancellationToken cancellationToken)
    {
        var sw = Stopwatch.StartNew();
        while (!await predicate())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (sw.Elapsed > TimeSpan.FromSeconds(20))
            {
                throw new TimeoutException("Condition not met within 20 seconds.");
            }

            await Task.Delay(20, cancellationToken);
        }
    }

    /// <summary>
    ///     Synchronous-predicate overload of <see cref="WaitForAsync(Func{Task{bool}}, CancellationToken)" />
    ///     for predicates that don't need async lookups (e.g. reading a <c>Volatile.Read</c> counter).
    /// </summary>
    public static Task WaitForAsync(Func<bool> predicate, CancellationToken cancellationToken)
        => WaitForAsync(() => Task.FromResult(predicate()), cancellationToken);
}
