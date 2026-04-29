using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;

namespace Surefire.Tests;

public sealed class SurefireLogEventPumpTests
{
    [Fact]
    public async Task DropRunState_RemovesPerRunBookkeeping()
    {
        var ct = TestContext.Current.CancellationToken;

        var time = new FakeTimeProvider(new(2025, 6, 15, 10, 0, 0, TimeSpan.Zero));
        var store = new InMemoryJobStore(time);
        var notifications = new InMemoryNotificationProvider(NullLogger<InMemoryNotificationProvider>.Instance);
        var options = new SurefireOptions();
        var instrumentation = new SurefireInstrumentation(new DummyMeterFactory());
        var pump = new SurefireLogEventPump(store, notifications, options, instrumentation, time,
            new(() => 0.5), NullLogger<SurefireLogEventPump>.Instance);

        var entry = new SurefireLogEventPump.LogEntry(
            "run-1",
            0,
            1,
            time.GetUtcNow(),
            "TestCategory",
            LogLevel.Information,
            0,
            null,
            "hello",
            null);

        Assert.True(pump.TryEnqueue(entry));
        pump.DropRunState("run-1");

        // After DropRunState, FlushRunAsync is a no-op (no state to wait on).
        await pump.FlushRunAsync("run-1", ct);
    }

    private sealed class DummyMeterFactory : IMeterFactory
    {
        private readonly Meter _meter = new("test");
        public Meter Create(MeterOptions options) => _meter;
        public void Dispose() => _meter.Dispose();
    }
}
