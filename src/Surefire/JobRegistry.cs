using System.Collections.Concurrent;
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

        var builder = new JobBuilder(definition);
        var registration = new RegisteredJob(name, handler, definition, builder.FilterTypes,
            builder.OnSuccessCallbacks, builder.OnRetryCallbacks, builder.OnDeadLetterCallbacks);

        _jobs[name] = registration;
        return builder;
    }

    public bool TryGet(string name, out RegisteredJob registration) =>
        _jobs.TryGetValue(name, out registration!);

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
    IReadOnlyList<Delegate> OnSuccessCallbacks,
    IReadOnlyList<Delegate> OnRetryCallbacks,
    IReadOnlyList<Delegate> OnDeadLetterCallbacks);