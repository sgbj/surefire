using System.Collections.Concurrent;

namespace Surefire;

public sealed class JobRegistry
{
    internal readonly ConcurrentDictionary<string, RegisteredJob> Jobs = new();

    internal void Register(RegisteredJob job)
    {
        Jobs[job.Definition.Name] = job;
        _cachedNames = null;
    }

    internal RegisteredJob? Get(string name) =>
        Jobs.TryGetValue(name, out var job) ? job : null;

    private IReadOnlyCollection<string>? _cachedNames;

    public IReadOnlyCollection<string> GetJobNames() =>
        _cachedNames ??= [.. Jobs.Keys];
}
