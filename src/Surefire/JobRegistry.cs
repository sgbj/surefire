using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

internal sealed class JobRegistry
{
    // `_jobs` is ordinal — case differences create distinct keys, which surfaces as two duplicate
    // rows on the dashboard or a primary-key violation on stores with case-insensitive collations
    // (SQL Server's default). The case-divergence guard in AddOrUpdate refuses that configuration
    // at registration time. `_writeGate` makes the read-then-write sequence in that guard atomic
    // against concurrent callers, which is required for the runtime-registration path.
    private readonly ConcurrentDictionary<string, RegisteredJob> _jobs =
        new(StringComparer.Ordinal);

    private readonly Lock _writeGate = new();

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
                // Re-check inside the lock so the duplicate-name guard and the write are atomic
                // against any concurrent AddOrUpdate. A re-registration of the exact same name is
                // allowed — it updates the handler.
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
        // Only return queue names that registered jobs actually use. "default" appears here only
        // when at least one job omits an explicit queue. This keeps the dashboard / heartbeat
        // honest: an app that defines its own queues won't surface a phantom "default" row, and
        // retention can sweep an unused "default" the same as any other stale queue.
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