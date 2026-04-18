using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

internal sealed class JobRegistry
{
    private readonly ConcurrentDictionary<string, RegisteredJob> _jobs =
        new(StringComparer.Ordinal);

    public JobBuilder AddOrUpdate(string name, Delegate handler, IServiceProvider services)
    {
        var definition = new JobDefinition { Name = name };
        var serviceChecker = services.GetService<IServiceProviderIsService>();
        definition.ArgumentsSchema = JobArgumentsSchemaBuilder.Build(
            handler,
            parameterType =>
                parameterType == typeof(IServiceProvider)
                || (serviceChecker?.IsService(parameterType) ?? false));

        var metadata = HandlerMetadata.Build(handler);
        JobBuilder? builder = null;

        void SyncRegistration()
        {
            _jobs[name] = new(
                name,
                handler,
                definition.Clone(),
                [.. builder!.FilterTypes],
                builder.OnSuccessCallbacks.Select(CompiledCallback.Build).ToArray(),
                builder.OnRetryCallbacks.Select(CompiledCallback.Build).ToArray(),
                builder.OnDeadLetterCallbacks.Select(CompiledCallback.Build).ToArray(),
                metadata);
        }

        builder = new(definition, SyncRegistration);
        SyncRegistration();
        return builder;
    }

    public bool TryGet(string name, [MaybeNullWhen(false)] out RegisteredJob registration) =>
        _jobs.TryGetValue(name, out registration);

    public IReadOnlyList<RegisteredJob> Snapshot() => [.. _jobs.Values];

    public IReadOnlyCollection<string> GetJobNames() => [.. _jobs.Keys];

    public IReadOnlyCollection<string> GetQueueNames()
    {
        var names = new HashSet<string>(StringComparer.Ordinal) { "default" };
        foreach (var registration in _jobs.Values)
        {
            names.Add(registration.Definition.Queue ?? "default");
        }

        return [.. names];
    }
}

internal sealed record RegisteredJob(
    string Name,
    Delegate Handler,
    JobDefinition Definition,
    IReadOnlyList<Type> FilterTypes,
    IReadOnlyList<CompiledCallback> OnSuccessCallbacks,
    IReadOnlyList<CompiledCallback> OnRetryCallbacks,
    IReadOnlyList<CompiledCallback> OnDeadLetterCallbacks,
    HandlerMetadata Metadata);