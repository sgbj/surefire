namespace Surefire.Tests;

public sealed class RetryPolicyTests
{
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
