using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

internal sealed class JobRegistry
{
    // Ordinal: case differences create distinct keys, which surfaces as duplicate dashboard rows
    // or PK violations on case-insensitive collations (SQL Server default). AddOrUpdate's
    // case-divergence guard refuses that configuration at registration time, with _writeGate
    // making the read-then-write atomic against concurrent registrations.
    private readonly ConcurrentDictionary<string, RegisteredJob> _jobs =
        new(StringComparer.Ordinal);

    private readonly Lock _writeGate = new();

    [RequiresUnreferencedCode("Reflects over a user-supplied handler delegate to build job metadata.")]
    [RequiresDynamicCode("Reflects over a user-supplied handler delegate to build job metadata.")]
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
            lock (_writeGate)
            {
                // Re-check under lock so the guard and the write are atomic against concurrent
                // AddOrUpdate. Re-registering the exact same name updates the handler.
                foreach (var existing in _jobs.Keys)
                {
                    if (string.Equals(existing, name, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    if (string.Equals(existing, name, StringComparison.OrdinalIgnoreCase))
                    {
                        throw new InvalidOperationException(
                            $"Job names '{existing}' and '{name}' differ only in case; Surefire treats them as " +
                            "distinct, which is almost always a configuration mistake.");
                    }
                }

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
        // Only return queues registered jobs actually use. "default" appears only when a job
        // omits an explicit queue, so an app with its own queues won't surface a phantom
        // "default" row and retention can sweep an unused "default" like any other stale queue.
        var names = new HashSet<string>(StringComparer.Ordinal);
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
