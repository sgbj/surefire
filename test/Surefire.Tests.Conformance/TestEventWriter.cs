using Microsoft.Extensions.Logging.Abstractions;

namespace Surefire.Tests.Testing;

/// <summary>
///     Starts and stops a <see cref="BatchedEventWriter" /> for tests so they exercise the exact
///     hosted-service code path production uses. Disposing the returned handle drains the worker
///     and releases resources, matching IHost shutdown semantics.
/// </summary>
internal sealed class TestEventWriter : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();

    private TestEventWriter(BatchedEventWriter writer) => Writer = writer;

    public BatchedEventWriter Writer { get; }

    public async ValueTask DisposeAsync()
    {
        try
        {
            await Writer.StopAsync(CancellationToken.None);
        }
        catch (OperationCanceledException)
        {
        }

        _cts.Dispose();
    }

    public static async Task<TestEventWriter> StartAsync(IJobStore store, INotificationProvider notifications)
    {
        var writer = new BatchedEventWriter(store, notifications, new(),
            TimeProvider.System, new(), NullLogger<BatchedEventWriter>.Instance);
        var handle = new TestEventWriter(writer);
        await writer.StartAsync(handle._cts.Token);
        return handle;
    }

    public static implicit operator BatchedEventWriter(TestEventWriter handle) => handle.Writer;
}
