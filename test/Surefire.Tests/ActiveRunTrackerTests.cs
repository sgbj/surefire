namespace Surefire.Tests;

/// <summary>
///     Covers the identity-keyed removal that keeps a durable suspend->resume cycle (which re-claims
///     the same run id on a fresh execution task) from tearing down the successor's cancellation
///     wiring when the prior task's teardown runs late.
/// </summary>
public sealed class ActiveRunTrackerTests
{
    private const string RunId = "run-1";

    [Fact]
    public void Remove_WithMatchingCts_RemovesRegistration()
    {
        var tracker = new ActiveRunTracker();
        using var cts = new CancellationTokenSource();
        tracker.Add(RunId, cts);

        tracker.Remove(RunId, cts);

        Assert.DoesNotContain(RunId, tracker.Snapshot());
    }

    [Fact]
    public void Remove_WithStaleCts_LeavesSuccessorRegistrationIntact()
    {
        var tracker = new ActiveRunTracker();
        using var first = new CancellationTokenSource();
        using var second = new CancellationTokenSource();

        // The prior execution registered `first`; the resumed execution re-claims the same id and
        // registers `second`. The prior task's late teardown must not evict the successor.
        tracker.Add(RunId, first);
        tracker.Add(RunId, second);

        tracker.Remove(RunId, first);

        Assert.Contains(RunId, tracker.Snapshot());

        // And the successor's cancellation wiring is still reachable.
        tracker.TryRequestCancel(RunId);
        Assert.True(second.IsCancellationRequested);
        Assert.False(first.IsCancellationRequested);
    }

    [Fact]
    public void Remove_WithMatchingCts_AfterSuccessorReclaim_IsNoOp()
    {
        var tracker = new ActiveRunTracker();
        using var first = new CancellationTokenSource();
        using var second = new CancellationTokenSource();

        tracker.Add(RunId, first);
        tracker.Remove(RunId, first);
        tracker.Add(RunId, second);

        // A second, stale teardown for `first` must not disturb the live `second` registration.
        tracker.Remove(RunId, first);

        Assert.Contains(RunId, tracker.Snapshot());
    }
}
