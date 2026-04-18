using Surefire.Tests.Conformance;

namespace Surefire.Tests.InMemory;

public sealed class InMemoryFixture : IStoreTestFixture
{
    Task<IJobStore> IStoreTestFixture.CreateStoreAsync() =>
        Task.FromResult<IJobStore>(new InMemoryJobStore(TimeProvider.System));

    Task IStoreTestFixture.CleanAsync() => Task.CompletedTask;
}