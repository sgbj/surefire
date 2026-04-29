using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Surefire.Tests.Fakes;

namespace Surefire.Tests;

public sealed class RetentionServiceTests
{
    private static (SurefireRetentionService service, FakePurgeStore store, FakeTimeProvider time)
        CreateService(SurefireOptions options)
    {
        var time = new FakeTimeProvider();
        time.SetUtcNow(new(2025, 6, 15, 10, 0, 0, TimeSpan.Zero));
        var store = new FakePurgeStore();
        var instrumentation = new SurefireInstrumentation(new DummyMeterFactory());
        var service = new SurefireRetentionService(
            store, options, time, instrumentation, new(time), new(() => 0.5),
            NullLogger<SurefireRetentionService>.Instance);
        return (service, store, time);
    }

    [Fact]
    public async Task RetentionPeriodSet_PurgesWithCorrectThreshold()
    {
        var retention = TimeSpan.FromDays(7);
        var (service, store, time) = CreateService(new() { RetentionPeriod = retention });
        var expectedThreshold = time.GetUtcNow() - retention;

        await service.RunRetentionAsync(CancellationToken.None);

        Assert.Single(store.PurgeThresholds);
        Assert.Equal(expectedThreshold, store.PurgeThresholds[0]);
    }

    [Fact]
    public async Task RetentionPeriodNull_PurgeNotCalled()
    {
        var (service, store, _) = CreateService(new() { RetentionPeriod = null });

        await service.RunRetentionAsync(CancellationToken.None);

        Assert.Empty(store.PurgeThresholds);
    }

    [Fact]
    public async Task MultipleTicks_PurgesEachTime()
    {
        var (service, store, time) = CreateService(new() { RetentionPeriod = TimeSpan.FromHours(1) });

        await service.RunRetentionAsync(CancellationToken.None);
        time.Advance(TimeSpan.FromMinutes(5));
        await service.RunRetentionAsync(CancellationToken.None);

        Assert.Equal(2, store.PurgeThresholds.Count);
        Assert.True(store.PurgeThresholds[1] > store.PurgeThresholds[0]);
    }

    private sealed class DummyMeterFactory : IMeterFactory
    {
        private readonly Meter _meter = new("test");
        public Meter Create(MeterOptions options) => _meter;
        public void Dispose() => _meter.Dispose();
    }

    private sealed class FakePurgeStore : ThrowingJobStore
    {
        public List<DateTimeOffset> PurgeThresholds { get; } = [];

        public override Task PurgeAsync(DateTimeOffset threshold, CancellationToken ct = default)
        {
            PurgeThresholds.Add(threshold);
            return Task.CompletedTask;
        }
    }
}
