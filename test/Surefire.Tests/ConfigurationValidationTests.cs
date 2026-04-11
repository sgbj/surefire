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

    [Fact]
    public void AddSurefire_InactiveThresholdLessThanTwiceHeartbeatInterval_Throws()
    {
        var services = new ServiceCollection();

        var ex = Assert.Throws<InvalidOperationException>(() =>
            services.AddSurefire(options =>
            {
                options.HeartbeatInterval = TimeSpan.FromSeconds(30);
                options.InactiveThreshold = TimeSpan.FromSeconds(30);
            }));

        Assert.Contains("2x", ex.Message);
    }

    [Fact]
    public void AddSurefire_InactiveThresholdExactlyTwiceHeartbeatInterval_Succeeds()
    {
        var services = new ServiceCollection();

        services.AddSurefire(options =>
        {
            options.HeartbeatInterval = TimeSpan.FromSeconds(30);
            options.InactiveThreshold = TimeSpan.FromSeconds(60);
        });
    }

    [Fact]
    public void AddSurefire_CopiesSerializerOptionsSnapshot()
    {
        var services = new ServiceCollection();
        SurefireOptions? configured = null;

        services.AddSurefire(options =>
        {
            configured = options;
            options.SerializerOptions.PropertyNameCaseInsensitive = false;
        });

        using var provider = services.BuildServiceProvider();
        var runtimeOptions = provider.GetRequiredService<SurefireOptions>();

        Assert.NotNull(configured);
        Assert.NotSame(configured!.SerializerOptions, runtimeOptions.SerializerOptions);
        Assert.False(runtimeOptions.SerializerOptions.PropertyNameCaseInsensitive);

        configured.SerializerOptions.PropertyNameCaseInsensitive = true;
        Assert.False(runtimeOptions.SerializerOptions.PropertyNameCaseInsensitive);
    }

    private static JobBuilder CreateJobBuilder()
    {
        var ctor = typeof(JobBuilder).GetConstructor(
            BindingFlags.Instance | BindingFlags.NonPublic,
            binder: null,
            [typeof(JobDefinition), typeof(Action)],
            modifiers: null)!;

        return (JobBuilder)ctor.Invoke([new JobDefinition { Name = "validation-job" }, () => { }]);
    }
}
