using System.Collections.Concurrent;

namespace Surefire;

public sealed class JobRegistry
{
    internal readonly ConcurrentDictionary<string, RegisteredJob> Jobs = new();

    internal void Register(RegisteredJob job)
    {
        Jobs[job.Definition.Name] = job;
        Volatile.Write(ref _cachedNames, null);
    }

    internal void Unregister(string name)
    {
        Jobs.TryRemove(name, out _);
        Volatile.Write(ref _cachedNames, null);
    }

    internal RegisteredJob? Get(string name) =>
        Jobs.TryGetValue(name, out var job) ? job : null;

    private IReadOnlyCollection<string>? _cachedNames;

    public IReadOnlyCollection<string> GetJobNames()
    {
        var names = Volatile.Read(ref _cachedNames);
        if (names is not null) return names;
        names = [.. Jobs.Keys];
        Volatile.Write(ref _cachedNames, names);
        return names;
    }
}
