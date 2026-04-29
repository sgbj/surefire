namespace Surefire.Tests;

public sealed class RetryPolicyTests
{
    [Fact]
    public void NextDelay_Fixed_UsesInitialDelayForAllAttempts()
    {
        var backoff = new Backoff(() => 1.0);
        var initial = TimeSpan.FromMilliseconds(120);
        var max = TimeSpan.FromSeconds(5);

        Assert.Equal(TimeSpan.FromMilliseconds(120),
            backoff.NextDelay(0, initial, max, backoffType: BackoffType.Fixed));
        Assert.Equal(TimeSpan.FromMilliseconds(120),
            backoff.NextDelay(1, initial, max, backoffType: BackoffType.Fixed));
        Assert.Equal(TimeSpan.FromMilliseconds(120),
            backoff.NextDelay(5, initial, max, backoffType: BackoffType.Fixed));
    }

    [Fact]
    public void NextDelay_Exponential_DoublesUntilMaxDelay()
    {
        var backoff = new Backoff(() => 1.0);
        var initial = TimeSpan.FromMilliseconds(100);
        var max = TimeSpan.FromMilliseconds(350);

        Assert.Equal(TimeSpan.FromMilliseconds(100), backoff.NextDelay(0, initial, max));
        Assert.Equal(TimeSpan.FromMilliseconds(200), backoff.NextDelay(1, initial, max));
        Assert.Equal(TimeSpan.FromMilliseconds(350), backoff.NextDelay(2, initial, max));
        Assert.Equal(TimeSpan.FromMilliseconds(350), backoff.NextDelay(3, initial, max));
    }

    [Fact]
    public void NextDelay_WithJitter_StaysWithinHalfToFullDelayRange()
    {
        var backoff = new Backoff();
        var initial = TimeSpan.FromMilliseconds(200);
        var max = TimeSpan.FromMilliseconds(200);

        var observed = new HashSet<long>();
        for (var i = 0; i < 32; i++)
        {
            var delay = backoff.NextDelay(0, initial, max);
            Assert.InRange(delay, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200));
            observed.Add(delay.Ticks);
        }

        Assert.True(observed.Count > 1, "Expected jitter to produce non-identical delays across calls.");
    }

    [Fact]
    public void NextDelay_Exponential_FirstAttemptEqualsInitialDelay()
    {
        var backoff = new Backoff(() => 1.0);
        var initial = TimeSpan.FromSeconds(5);
        var max = TimeSpan.FromMinutes(5);

        Assert.Equal(TimeSpan.FromSeconds(5), backoff.NextDelay(0, initial, max));
        Assert.Equal(TimeSpan.FromSeconds(10), backoff.NextDelay(1, initial, max));
        Assert.Equal(TimeSpan.FromSeconds(20), backoff.NextDelay(2, initial, max));
    }

    [Fact]
    public void NextDelay_ExponentialHugeAttempt_ClampsToMaxDelay()
    {
        var backoff = new Backoff(() => 1.0);
        var initial = TimeSpan.FromMilliseconds(1);
        var max = TimeSpan.FromSeconds(10);

        var delay = backoff.NextDelay(int.MaxValue, initial, max);

        Assert.Equal(TimeSpan.FromSeconds(10), delay);
    }

    [Fact]
    public void NextDelay_JitterAtMinimum_HalvesDelay()
    {
        var backoff = new Backoff(() => 0.0);
        var initial = TimeSpan.FromMilliseconds(200);
        var max = TimeSpan.FromSeconds(5);

        var delay = backoff.NextDelay(0, initial, max);

        Assert.Equal(TimeSpan.FromMilliseconds(100), delay);
    }

    [Fact]
    public void NextDelay_JitterDisabled_ReturnsExactDelay()
    {
        var backoff = new Backoff(() => 0.0);
        var initial = TimeSpan.FromMilliseconds(200);
        var max = TimeSpan.FromSeconds(5);

        var delay = backoff.NextDelay(0, initial, max, false);

        Assert.Equal(TimeSpan.FromMilliseconds(200), delay);
    }

    [Fact]
    public void NextDelay_ExponentialBoundaryShift_RemainsDeterministicAtLimit()
    {
        var backoff = new Backoff(() => 1.0);
        var initialTicks = 1L << 20;
        var maxTicks = 1L << 62;
        var maxShift = 62 - (int)Math.Floor(Math.Log2(initialTicks));

        var initial = TimeSpan.FromTicks(initialTicks);
        var max = TimeSpan.FromTicks(maxTicks);

        var boundaryDelay = backoff.NextDelay(maxShift, initial, max);
        var beyondBoundaryDelay = backoff.NextDelay(maxShift + 1, initial, max);

        Assert.Equal(TimeSpan.FromTicks(maxTicks), boundaryDelay);
        Assert.Equal(TimeSpan.FromTicks(maxTicks), beyondBoundaryDelay);
    }
}
