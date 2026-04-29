using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Surefire.Tests.Testing;

public static class TestWait
{
    public static Task<T> PollUntilAsync<T>(
        Func<Task<T?>> probe,
        Func<T, bool> done,
        TimeSpan timeout,
        TimeSpan interval,
        string timeoutMessage,
        CancellationToken cancellationToken = default)
        where T : class
    {
        return PollUntilAsync(_ => probe(), done, timeout, interval, timeoutMessage, cancellationToken);
    }

    public static async Task<T> PollUntilAsync<T>(
        Func<CancellationToken, Task<T?>> probe,
        Func<T, bool> done,
        TimeSpan timeout,
        TimeSpan interval,
        string timeoutMessage,
        CancellationToken cancellationToken = default)
        where T : class
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);

        try
        {
            while (true)
            {
                timeoutCts.Token.ThrowIfCancellationRequested();

                var current = await probe(timeoutCts.Token);
                if (current is { } && done(current))
                {
                    return current;
                }

                await Task.Delay(interval, timeoutCts.Token);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(timeoutMessage);
        }
    }

    public static async Task<JobRun> AwaitRunAsync(
        IJobStore store,
        INotificationProvider notifications,
        string runId,
        Func<JobRun, bool> predicate,
        TimeSpan timeout,
        string? timeoutMessage = null,
        CancellationToken cancellationToken = default)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);

        using var wakeup = new SemaphoreSlim(0, 1);
        Func<string?, Task> signal = _ =>
        {
            try
            {
                wakeup.Release();
            }
            catch (SemaphoreFullException)
            {
            }
            catch (ObjectDisposedException)
            {
            }

            return Task.CompletedTask;
        };

        await using var sub1 = await notifications.SubscribeAsync(
            NotificationChannels.RunTerminated(runId), signal, timeoutCts.Token);
        await using var sub2 = await notifications.SubscribeAsync(
            NotificationChannels.RunEvent(runId), signal, timeoutCts.Token);
        await using var sub3 = await notifications.SubscribeAsync(
            NotificationChannels.RunAvailable, signal, timeoutCts.Token);
        await using var sub4 = await notifications.SubscribeAsync(
            NotificationChannels.RunCreated, signal, timeoutCts.Token);

        try
        {
            while (true)
            {
                timeoutCts.Token.ThrowIfCancellationRequested();

                var run = await store.GetRunAsync(runId, timeoutCts.Token);
                if (run is { } && predicate(run))
                {
                    return run;
                }

                await wakeup.WaitAsync(TimeSpan.FromMilliseconds(500), timeoutCts.Token);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(timeoutMessage ?? $"Timed out waiting for run '{runId}' to satisfy predicate.");
        }
    }

    public static async Task PollUntilConditionAsync(
        Func<CancellationToken, Task<bool>> probe,
        TimeSpan timeout,
        TimeSpan interval,
        string timeoutMessage,
        CancellationToken cancellationToken = default)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCts.CancelAfter(timeout);

        try
        {
            while (true)
            {
                timeoutCts.Token.ThrowIfCancellationRequested();

                if (await probe(timeoutCts.Token))
                {
                    return;
                }

                await Task.Delay(interval, timeoutCts.Token);
            }
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            throw new TimeoutException(timeoutMessage);
        }
    }
}

public sealed class TestHost(IServiceProvider services) : IHost
{
    public IServiceProvider Services { get; } = services;

    public Task StartAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public void Dispose()
    {
        if (Services is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }
}

public sealed class RuntimeHarness : IAsyncDisposable
{
    private readonly bool _disposeProvider;
    private readonly IReadOnlyList<IHostedService> _hostedServices;

    internal RuntimeHarness(
        ServiceProvider provider,
        IHost host,
        IJobStore store,
        INotificationProvider notifications,
        IJobClient client,
        IReadOnlyList<IHostedService> hostedServices,
        bool disposeProvider = true)
        : this(provider, host, store, notifications, client, Channel.CreateUnbounded<bool>(), hostedServices,
            disposeProvider)
    {
    }

    internal RuntimeHarness(
        ServiceProvider provider,
        IHost host,
        IJobStore store,
        INotificationProvider notifications,
        IJobClient client,
        Channel<bool> permits,
        IReadOnlyList<IHostedService> hostedServices,
        bool disposeProvider = true)
    {
        Provider = provider;
        Host = host;
        Store = store;
        Notifications = notifications;
        Client = client;
        Permits = permits;
        _hostedServices = hostedServices;
        _disposeProvider = disposeProvider;
    }

    public ServiceProvider Provider { get; }
    public IHost Host { get; }
    internal IJobStore Store { get; }
    internal INotificationProvider Notifications { get; }
    public IJobClient Client { get; }
    public Channel<bool> Permits { get; }

    public async ValueTask DisposeAsync()
    {
        await HostedServiceLifecycle.StopAsync(_hostedServices);
        if (_disposeProvider)
        {
            await Provider.DisposeAsync();
        }
    }

    public Task StartAsync(CancellationToken cancellationToken = default) =>
        HostedServiceLifecycle.StartAsync(_hostedServices, cancellationToken);

    private static class HostedServiceLifecycle
    {
        public static async Task StartAsync(IReadOnlyList<IHostedService> hostedServices,
            CancellationToken cancellationToken = default)
        {
            foreach (var service in hostedServices)
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StartingAsync(cancellationToken);
                }
            }

            foreach (var service in hostedServices)
            {
                await service.StartAsync(cancellationToken);
            }

            foreach (var service in hostedServices)
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StartedAsync(cancellationToken);
                }
            }
        }

        public static async Task StopAsync(IReadOnlyList<IHostedService> hostedServices)
        {
            foreach (var service in hostedServices.Reverse())
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StoppingAsync(CancellationToken.None);
                }
            }

            foreach (var service in hostedServices.Reverse())
            {
                await service.StopAsync(CancellationToken.None);
            }

            foreach (var service in hostedServices.Reverse())
            {
                if (service is IHostedLifecycleService lifecycle)
                {
                    await lifecycle.StoppedAsync(CancellationToken.None);
                }
            }
        }
    }
}
