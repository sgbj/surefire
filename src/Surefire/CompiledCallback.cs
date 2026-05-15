namespace Surefire;

/// <summary>
///     Pre-compiled lifecycle callback invoker. Built once from a <see cref="CallbackDescriptor" />
///     so per-invocation work is a function-pointer call. Callback return values are always
///     discarded; any return shape is supported so user code can borrow its preferred signature
///     without ceremony.
/// </summary>
internal sealed class CompiledCallback
{
    private readonly CallbackDescriptor _descriptor;

    private CompiledCallback(CallbackDescriptor descriptor) => _descriptor = descriptor;

    public static CompiledCallback FromDescriptor(CallbackDescriptor descriptor) => new(descriptor);

    /// <summary>Binds parameters, invokes the compiled delegate, and awaits the result.</summary>
    public async Task InvokeAsync(JobContext context, IServiceProvider services, CancellationToken cancellationToken)
    {
        var args = BindArgs(context, services, cancellationToken);
        var returned = _descriptor.Invoke(args, _descriptor.Handler);

        // Match the user's method signature: await tasks, discard everything else. Sync scalar
        // returns are produced eagerly inside the Invoke call; iterators (IAsyncEnumerable<T>)
        // don't run their body until enumerated and are effectively no-ops, which matches the
        // language semantics rather than fighting them.
        switch (returned)
        {
            case null:
                return;
            case Task task:
                await task;
                return;
            case ValueTask valueTask:
                await valueTask;
                return;
        }

        if (_descriptor.AsTask is { } asTask)
        {
            await asTask(returned);
        }
    }

    private object?[] BindArgs(JobContext context, IServiceProvider services, CancellationToken cancellationToken)
    {
        var parameters = _descriptor.Parameters;
        var args = new object?[parameters.Count];
        for (var i = 0; i < parameters.Count; i++)
        {
            var p = parameters[i];

            switch (p.Kind)
            {
                case ParameterKind.JobContext:
                    args[i] = context;
                    continue;
                case ParameterKind.CancellationToken:
                    args[i] = cancellationToken;
                    continue;
                case ParameterKind.ServiceProvider:
                    args[i] = services;
                    continue;
            }

            if (context.Exception is { } ex && p.Type.IsInstanceOfType(ex))
            {
                args[i] = ex;
                continue;
            }

            if (context.Result is { } result && p.Type.IsInstanceOfType(result))
            {
                args[i] = result;
                continue;
            }

            var service = services.GetService(p.Type);
            if (service is { })
            {
                args[i] = service;
                continue;
            }

            if (p.HasDefault)
            {
                args[i] = p.DefaultValue;
                continue;
            }

            throw new InvalidOperationException(
                $"Unable to bind callback parameter '{p.Name}' for job '{context.JobName}'.");
        }

        return args;
    }
}
