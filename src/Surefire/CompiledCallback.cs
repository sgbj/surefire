using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Runtime.ExceptionServices;

namespace Surefire;

/// <summary>
///     Pre-compiled lifecycle callback invoker. Built once per registered callback at registration
///     time to avoid <see cref="Delegate.DynamicInvoke" />, <see cref="MethodBase.GetParameters()" />,
///     and per-call reflection for ValueTask&lt;T&gt; awaiting on every invocation.
/// </summary>
internal sealed class CompiledCallback
{
    private readonly Func<object, Task>? _asTask; // non-null for ValueTask<T> returns
    private readonly Func<object?[], object?> _invoke;
    private readonly ParameterInfo[] _parameters;
    private readonly ReturnKind _returnKind;

    private CompiledCallback(
        Func<object?[], object?> invoke,
        ParameterInfo[] parameters,
        ReturnKind returnKind,
        Func<object, Task>? asTask)
    {
        _invoke = invoke;
        _parameters = parameters;
        _returnKind = returnKind;
        _asTask = asTask;
    }

    /// <summary>Compiles a callback delegate into a fast invoker. Call once at registration time.</summary>
    [RequiresUnreferencedCode("Reflects over a user-supplied callback delegate.")]
    [RequiresDynamicCode("Reflects over a user-supplied callback delegate.")]
    public static CompiledCallback Build(Delegate callback)
    {
        var parameters = callback.Method.GetParameters();
        var invoke = DelegateCompiler.CompileInvoke(callback, parameters);
        var returnType = callback.Method.ReturnType;
        var returnKind = ClassifyReturn(returnType);
        var asTask = returnKind == ReturnKind.ValueTaskOfT ? DelegateCompiler.CompileAsTask(returnType) : null;

        return new(invoke, parameters, returnKind, asTask);
    }

    /// <summary>Binds parameters, invokes the compiled delegate, and awaits the result.</summary>
    public async Task InvokeAsync(JobContext context, IServiceProvider services, CancellationToken cancellationToken)
    {
        var args = BindArgs(context, services, cancellationToken);

        object? returned;
        try
        {
            returned = _invoke(args);
        }
        catch (TargetInvocationException ex) when (ex.InnerException is { })
        {
            ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
            return; // unreachable
        }

        switch (_returnKind)
        {
            case ReturnKind.Void:
                return;
            case ReturnKind.Task:
                await (Task)returned!;
                return;
            case ReturnKind.ValueTask:
                await (ValueTask)returned!;
                return;
            case ReturnKind.ValueTaskOfT:
                await _asTask!(returned!);
                return;
        }
    }

    private object?[] BindArgs(JobContext context, IServiceProvider services, CancellationToken cancellationToken)
    {
        var args = new object?[_parameters.Length];
        for (var i = 0; i < _parameters.Length; i++)
        {
            var p = _parameters[i];

            if (p.ParameterType == typeof(JobContext))
            {
                args[i] = context;
                continue;
            }

            if (p.ParameterType == typeof(CancellationToken))
            {
                args[i] = cancellationToken;
                continue;
            }

            if (context.Exception is { } ex && p.ParameterType.IsInstanceOfType(ex))
            {
                args[i] = ex;
                continue;
            }

            if (context.Result is { } result && p.ParameterType.IsInstanceOfType(result))
            {
                args[i] = result;
                continue;
            }

            var service = services.GetService(p.ParameterType);
            if (service is { })
            {
                args[i] = service;
                continue;
            }

            if (p.HasDefaultValue)
            {
                args[i] = p.DefaultValue;
                continue;
            }

            throw new InvalidOperationException(
                $"Unable to bind callback parameter '{p.Name}' for job '{context.JobName}'.");
        }

        return args;
    }

    private static ReturnKind ClassifyReturn(Type returnType)
    {
        if (returnType == typeof(void))
        {
            return ReturnKind.Void;
        }

        if (returnType == typeof(Task))
        {
            return ReturnKind.Task;
        }

        if (returnType == typeof(ValueTask))
        {
            return ReturnKind.ValueTask;
        }

        if (returnType.IsGenericType && returnType.GetGenericTypeDefinition() == typeof(ValueTask<>))
        {
            return ReturnKind.ValueTaskOfT;
        }

        // Task<T> is still a Task; the base case handles it.
        if (typeof(Task).IsAssignableFrom(returnType))
        {
            return ReturnKind.Task;
        }

        return ReturnKind.Void;
    }

    private enum ReturnKind
    {
        Void,
        Task,
        ValueTask,
        ValueTaskOfT
    }
}
