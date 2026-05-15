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

    /// <summary>
    ///     Registers a job from a pre-built descriptor. The descriptor carries parameter metadata,
    ///     the invocation shim, materializer, and stream binders, so this method itself doesn't
    ///     reflect over the handler delegate.
    /// </summary>
    public JobBuilder AddOrUpdate(JobRegistrationDescriptor descriptor, IServiceProvider services)
    {
        var definition = new JobDefinition { Name = descriptor.Name };
        var optionsAccessor = services.GetRequiredService<SurefireOptions>();
        definition.ArgumentsSchema = descriptor.ArgumentsSchema
                                     ?? JobArgumentsSchemaBuilder.BuildFromDescriptor(
                                         descriptor.Parameters,
                                         descriptor.ParameterJsonTypeInfoFactories,
                                         optionsAccessor.SerializerOptions,
                                         services);

        return RegisterCore(descriptor.Name, descriptor, definition);
    }

    private JobBuilder RegisterCore(string name, JobRegistrationDescriptor descriptor, JobDefinition definition)
    {
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
                    descriptor,
                    definition.Clone(),
                    [.. builder!.FilterFactories],
                    [.. builder.OnSuccessCallbacks],
                    [.. builder.OnRetryCallbacks],
                    [.. builder.OnDeadLetterCallbacks]);
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
    JobRegistrationDescriptor Descriptor,
    JobDefinition Definition,
    IReadOnlyList<Func<IServiceProvider, IJobFilter>> FilterFactories,
    IReadOnlyList<CompiledCallback> OnSuccessCallbacks,
    IReadOnlyList<CompiledCallback> OnRetryCallbacks,
    IReadOnlyList<CompiledCallback> OnDeadLetterCallbacks);
