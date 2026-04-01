using Microsoft.Extensions.DependencyInjection;
using System.Reflection;

namespace Surefire.Tests;

public sealed class ConfigurationValidationTests
{
    [Fact]
    public void AddSurefire_InvalidPollingInterval_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddSurefire(options => options.PollingInterval = TimeSpan.Zero));
    }

    [Fact]
    public void AddSurefire_InvalidMaxNodeConcurrency_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddSurefire(options => options.MaxNodeConcurrency = 0));
    }

    [Fact]
    public void AddSurefire_InvalidRateLimiter_Throws()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            services.AddSurefire(options => options.AddFixedWindowLimiter("api", 0, TimeSpan.FromSeconds(1))));
    }

    [Fact]
    public void RetryPolicyBuilder_InvalidBounds_Throws()
    {
        var job = CreateJobBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            job.WithRetry(policy =>
            {
                policy.MaxRetries = 1;
                policy.InitialDelay = TimeSpan.FromSeconds(10);
                policy.MaxDelay = TimeSpan.FromSeconds(5);
            }));
    }

    [Fact]
    public void JobBuilder_InvalidValues_Throw()
    {
        var job = CreateJobBuilder();

        Assert.Throws<ArgumentOutOfRangeException>(() => job.WithMaxConcurrency(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => job.WithTimeout(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => job.WithRetry(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => job.WithMisfirePolicy(MisfirePolicy.FireAll, 0));
        Assert.Throws<ArgumentException>(() => job.WithQueue(" "));
        Assert.Throws<ArgumentException>(() => job.WithRateLimit(" "));
    }

    private static JobBuilder CreateJobBuilder()
    {
        var ctor = typeof(JobBuilder).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [typeof(JobDefinition)],
            modifiers: null)!;

        return (JobBuilder)ctor.Invoke([new JobDefinition { Name = "validation-job" }]);
    }
}
