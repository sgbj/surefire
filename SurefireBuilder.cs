using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

public sealed class SurefireBuilder
{
    internal IServiceCollection Services { get; }
    internal SurefireOptions Options { get; } = new();

    internal SurefireBuilder(IServiceCollection services) => Services = services;

    public string NodeName { get => Options.NodeName; set => Options.NodeName = value; }
    public TimeSpan PollingInterval { get => Options.PollingInterval; set => Options.PollingInterval = value; }
    public TimeSpan HeartbeatInterval { get => Options.HeartbeatInterval; set => Options.HeartbeatInterval = value; }
    public TimeSpan InactiveThreshold { get => Options.InactiveThreshold; set => Options.InactiveThreshold = value; }
    public TimeSpan? RetentionPeriod { get => Options.RetentionPeriod; set => Options.RetentionPeriod = value; }
    public TimeSpan RetentionCheckInterval { get => Options.RetentionCheckInterval; set => Options.RetentionCheckInterval = value; }
    public TimeSpan ShutdownTimeout { get => Options.ShutdownTimeout; set => Options.ShutdownTimeout = value; }
    public JsonSerializerOptions SerializerOptions { get => Options.SerializerOptions; set => Options.SerializerOptions = value; }
    public int? MaxNodeConcurrency { get => Options.MaxNodeConcurrency; set => Options.MaxNodeConcurrency = value; }
    public bool AutoMigrate { get => Options.AutoMigrate; set => Options.AutoMigrate = value; }

    public SurefireBuilder OnSuccess(Delegate callback) { Options.OnSuccess(callback); return this; }
    public SurefireBuilder OnFailure(Delegate callback) { Options.OnFailure(callback); return this; }
    public SurefireBuilder OnRetry(Delegate callback) { Options.OnRetry(callback); return this; }
    public SurefireBuilder OnDeadLetter(Delegate callback) { Options.OnDeadLetter(callback); return this; }

    public SurefireBuilder UseFilter<T>() where T : class, IJobFilter
    {
        Services.AddTransient<T>();
        Options.FilterTypes.Add(typeof(T));
        return this;
    }

    public QueueBuilder AddQueue(string name)
    {
        var builder = new QueueBuilder(name);
        Options.Queues.Add(builder.Definition);
        return builder;
    }

    public RateLimitBuilder AddRateLimit(string name)
    {
        var builder = new RateLimitBuilder(name);
        Options.RateLimits.Add(builder.Definition);
        return builder;
    }
}
