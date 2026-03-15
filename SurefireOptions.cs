using System.Text.Json;

namespace Surefire;

public sealed class SurefireOptions
{
    internal List<Func<IServiceProvider, JobContext, Task>> OnSuccessCallbacks { get; } = [];
    internal List<Func<IServiceProvider, JobContext, Task>> OnFailureCallbacks { get; } = [];
    internal List<Func<IServiceProvider, JobContext, Task>> OnRetryCallbacks { get; } = [];
    internal List<Func<IServiceProvider, JobContext, Task>> OnDeadLetterCallbacks { get; } = [];
    internal List<Type> FilterTypes { get; } = [];
    internal List<QueueDefinition> Queues { get; } = [];
    internal List<RateLimitDefinition> RateLimits { get; } = [];

    public string NodeName { get; set; } = Environment.MachineName;
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(30);
    public TimeSpan InactiveThreshold { get; set; } = TimeSpan.FromMinutes(2);
    public TimeSpan? RetentionPeriod { get; set; } = TimeSpan.FromDays(7);
    public TimeSpan RetentionCheckInterval { get; set; } = TimeSpan.FromMinutes(5);
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(15);
    public JsonSerializerOptions SerializerOptions { get; set; } = new(JsonSerializerOptions.Web)
    {
        // JSONB stores (PostgreSQL, etc.) sort object keys alphabetically, which can move
        // polymorphic type discriminators after other properties. Without this flag,
        // System.Text.Json requires discriminators to appear before any other properties.
        AllowOutOfOrderMetadataProperties = true
    };
    public int? MaxNodeConcurrency { get; set; }
    public bool AutoMigrate { get; set; } = true;

    public SurefireOptions OnSuccess(Delegate callback)
    {
        OnSuccessCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public SurefireOptions OnFailure(Delegate callback)
    {
        OnFailureCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public SurefireOptions OnRetry(Delegate callback)
    {
        OnRetryCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }

    public SurefireOptions OnDeadLetter(Delegate callback)
    {
        OnDeadLetterCallbacks.Add(ParameterBinder.CompileCallback(callback));
        return this;
    }
}
