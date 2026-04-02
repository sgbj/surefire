namespace Surefire.Tests;

public sealed class RetryPolicyTests
{
    [Fact]
    public void GetDelay_FixedBackoff_UsesInitialDelayForAllAttempts()
    {
        var policy = new RetryPolicy
        {
            BackoffType = BackoffType.Fixed,
            InitialDelay = TimeSpan.FromMilliseconds(120),
            MaxDelay = TimeSpan.FromSeconds(5),
            Jitter = false
        };

        Assert.Equal(TimeSpan.FromMilliseconds(120), policy.GetDelay(0));
        Assert.Equal(TimeSpan.FromMilliseconds(120), policy.GetDelay(1));
        Assert.Equal(TimeSpan.FromMilliseconds(120), policy.GetDelay(5));
    }

    [Fact]
    public void GetDelay_Exponential_DoublesUntilMaxDelay()
    {
        var policy = new RetryPolicy
        {
            BackoffType = BackoffType.Exponential,
            InitialDelay = TimeSpan.FromMilliseconds(100),
            MaxDelay = TimeSpan.FromMilliseconds(350),
            Jitter = false
        };

        Assert.Equal(TimeSpan.FromMilliseconds(100), policy.GetDelay(1));
        Assert.Equal(TimeSpan.FromMilliseconds(200), policy.GetDelay(2));
        Assert.Equal(TimeSpan.FromMilliseconds(350), policy.GetDelay(3));
        Assert.Equal(TimeSpan.FromMilliseconds(350), policy.GetDelay(4));
    }

    [Fact]
    public void GetDelay_WithJitter_StaysWithinHalfToFullDelayRange()
    {
        var policy = new RetryPolicy
        {
            BackoffType = BackoffType.Fixed,
            InitialDelay = TimeSpan.FromMilliseconds(200),
            MaxDelay = TimeSpan.FromMilliseconds(200),
            Jitter = true
        };

        var observed = new HashSet<long>();
        for (var i = 0; i < 32; i++)
        {
            var delay = policy.GetDelay(1);
            Assert.InRange(delay, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(200));
            observed.Add(delay.Ticks);
        }

        Assert.True(observed.Count > 1, "Expected jitter to produce non-identical delays across calls.");
    }

    [Fact]
    public void GetDelay_ExponentialHugeAttempt_ClampsToMaxDelay()
    {
        var policy = new RetryPolicy
        {
            BackoffType = BackoffType.Exponential,
            InitialDelay = TimeSpan.FromMilliseconds(1),
            MaxDelay = TimeSpan.FromSeconds(10),
            Jitter = false
        };

        var delay = policy.GetDelay(int.MaxValue);

        Assert.Equal(TimeSpan.FromSeconds(10), delay);
    }

    [Fact]
    public void GetDelay_ExponentialBoundaryShift_RemainsDeterministicAtLimit()
    {
        var initialTicks = 1L << 20;
        var maxTicks = 1L << 62;
        var maxShift = 62 - (int)Math.Floor(Math.Log2(initialTicks));

        var policy = new RetryPolicy
        {
            BackoffType = BackoffType.Exponential,
            InitialDelay = TimeSpan.FromTicks(initialTicks),
            MaxDelay = TimeSpan.FromTicks(maxTicks),
            Jitter = false
        };

        var boundaryDelay = policy.GetDelay(maxShift + 1);
        var beyondBoundaryDelay = policy.GetDelay(maxShift + 2);

        Assert.Equal(TimeSpan.FromTicks(maxTicks), boundaryDelay);
        Assert.Equal(TimeSpan.FromTicks(maxTicks), beyondBoundaryDelay);
    }
}
