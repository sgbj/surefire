using Surefire.Tests.Conformance;

namespace Surefire.Tests.InMemory;

public sealed class InMemoryFixture : IStoreTestFixture
{
    public Task<IJobStore> CreateStoreAsync() =>
        Task.FromResult<IJobStore>(new InMemoryJobStore(TimeProvider.System));

    public Task CleanAsync() => Task.CompletedTask;
}