namespace Surefire;

internal sealed class RegisteredJob
{
    public required JobDefinition Definition { get; init; }
    public required Func<IServiceProvider, JobContext, string?, Task<object?>> CompiledHandler { get; init; }
    public bool HasReturnValue { get; init; }
    public List<Func<IServiceProvider, JobContext, Task>> OnSuccessCallbacks { get; } = [];
    public List<Func<IServiceProvider, JobContext, Task>> OnFailureCallbacks { get; } = [];
    public List<Func<IServiceProvider, JobContext, Task>> OnRetryCallbacks { get; } = [];
    public List<Func<IServiceProvider, JobContext, Task>> OnDeadLetterCallbacks { get; } = [];
}
