using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

internal static class ParameterBinder
{
    public static (Func<IServiceProvider, JobContext, string?, Task<object?>> Handler, bool HasReturnValue) Compile(Delegate handler, JsonSerializerOptions serializerOptions)
    {
        var method = handler.Method;
        var parameters = method.GetParameters();
        var target = handler.Target;
        var hasReturnValue = DetermineHasReturnValue(method);

        var compiled = new Func<IServiceProvider, JobContext, string?, Task<object?>>(async (sp, ctx, argumentsJson) =>
        {
            var isService = sp.GetRequiredService<IServiceProviderIsService>();
            var args = new object?[parameters.Length];
            JsonElement? argsElement = null;

            if (argumentsJson is not null)
            {
                argsElement = JsonSerializer.Deserialize<JsonElement>(argumentsJson);
            }

            for (var i = 0; i < parameters.Length; i++)
            {
                args[i] = ResolveParameter(parameters[i], sp, ctx, isService, argsElement, serializerOptions);
            }

            object? result;
            try
            {
                result = method.Invoke(target, args);
            }
            catch (TargetInvocationException tie) when (tie.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
                return null; // unreachable
            }

            if (result is Task task)
            {
                await task;
                var taskType = task.GetType();
                if (taskType.IsGenericType)
                {
                    return taskType.GetProperty("Result")?.GetValue(task);
                }
                return null;
            }

            if (result is ValueTask vt)
            {
                await vt;
                return null;
            }

            var resultType = result?.GetType();
            if (resultType is { IsGenericType: true } && resultType.GetGenericTypeDefinition() == typeof(ValueTask<>))
            {
                var asTask = (Task)resultType.GetMethod("AsTask")!.Invoke(result, null)!;
                await asTask;
                return asTask.GetType().GetProperty("Result")!.GetValue(asTask);
            }

            return result;
        });

        return (compiled, hasReturnValue);
    }

    public static Func<IServiceProvider, JobContext, Task> CompileCallback(Delegate callback)
    {
        var method = callback.Method;
        var parameters = method.GetParameters();
        var target = callback.Target;

        return async (sp, ctx) =>
        {
            var isService = sp.GetRequiredService<IServiceProviderIsService>();
            var args = new object?[parameters.Length];

            for (var i = 0; i < parameters.Length; i++)
            {
                args[i] = ResolveParameter(parameters[i], sp, ctx, isService);
            }

            object? result;
            try
            {
                result = method.Invoke(target, args);
            }
            catch (TargetInvocationException tie) when (tie.InnerException is not null)
            {
                ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
                return; // unreachable
            }

            if (result is Task task)
                await task;
            else if (result is ValueTask vt)
                await vt;
            else if (result?.GetType() is { IsGenericType: true } rt && rt.GetGenericTypeDefinition() == typeof(ValueTask<>))
            {
                var asTask = (Task)rt.GetMethod("AsTask")!.Invoke(result, null)!;
                await asTask;
            }
        };
    }

    private static bool DetermineHasReturnValue(MethodInfo method)
    {
        var returnType = method.ReturnType;
        return returnType != typeof(void) && returnType != typeof(Task) && returnType != typeof(ValueTask);
    }

    private static object? ResolveParameter(ParameterInfo param, IServiceProvider sp, JobContext ctx, IServiceProviderIsService isService, JsonElement? argsJson = null, JsonSerializerOptions? serializerOptions = null)
    {
        var type = param.ParameterType;

        if (type == typeof(CancellationToken))
            return ctx.CancellationToken;

        if (type == typeof(JobContext))
            return ctx;

        if (isService.IsService(type))
            return sp.GetRequiredService(type);

        if (argsJson is { ValueKind: JsonValueKind.Object } json && serializerOptions is not null)
        {
            foreach (var prop in json.EnumerateObject())
            {
                if (string.Equals(prop.Name, param.Name, StringComparison.OrdinalIgnoreCase))
                {
                    return prop.Value.Deserialize(type, serializerOptions);
                }
            }
        }

        if (param.HasDefaultValue)
            return param.DefaultValue;

        throw new InvalidOperationException(
            $"Cannot resolve parameter '{param.Name}' of type '{type.Name}'. " +
            "It is not a known type (CancellationToken, JobContext), not registered in DI, " +
            "and has no default value.");
    }
}
