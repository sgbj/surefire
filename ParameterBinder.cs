using System.Reflection;
using System.Runtime.ExceptionServices;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;

namespace Surefire;

internal sealed class BindingResult
{
    public required Func<IServiceProvider, JobContext, string?, Task<object?>> Handler { get; init; }
    public bool HasReturnValue { get; init; }
}

internal static class ParameterBinder
{
    public static BindingResult Compile(Delegate handler, JsonSerializerOptions serializerOptions)
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

        return new BindingResult
        {
            Handler = compiled,
            HasReturnValue = hasReturnValue
        };
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

        // IAsyncEnumerable<T> parameter: try JSON array first, then create event-based enumerator
        if (StreamingHelper.IsAsyncEnumerable(type) && serializerOptions is not null)
        {
            var itemType = type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>)
                ? type.GetGenericArguments()[0]
                : type.GetInterfaces().First(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>)).GetGenericArguments()[0];

            // Check if there's a matching JSON property with an array value
            if (argsJson is { ValueKind: JsonValueKind.Object } json)
            {
                foreach (var prop in json.EnumerateObject())
                {
                    if (string.Equals(prop.Name, param.Name, StringComparison.OrdinalIgnoreCase)
                        && prop.Value.ValueKind == JsonValueKind.Array)
                    {
                        // Deserialize as List<T> then wrap as IAsyncEnumerable<T>
                        var listType = typeof(List<>).MakeGenericType(itemType);
                        var list = prop.Value.Deserialize(listType, serializerOptions)!;
                        var toAsync = typeof(StreamingHelper)
                            .GetMethod(nameof(StreamingHelper.ToAsyncEnumerable), BindingFlags.Static | BindingFlags.NonPublic)!
                            .MakeGenericMethod(itemType);
                        return toAsync.Invoke(null, [list]);
                    }
                }
            }

            // Check for plan stream reference ($planStream marker stored by PlanEngine)
            if (argsJson is { ValueKind: JsonValueKind.Object } streamJson)
            {
                foreach (var streamProp in streamJson.EnumerateObject())
                {
                    if (string.Equals(streamProp.Name, param.Name, StringComparison.OrdinalIgnoreCase)
                        && streamProp.Value.ValueKind == JsonValueKind.Object
                        && streamProp.Value.TryGetProperty("$planStream", out var planStreamProp)
                        && planStreamProp.ValueKind == JsonValueKind.String)
                    {
                        var planSourceRunId = planStreamProp.GetString()!;
                        var planStreamType = typeof(PlanStreamEnumerable<>).MakeGenericType(itemType);
                        return Activator.CreateInstance(planStreamType,
                            planSourceRunId, ctx.Store, ctx.Notifications, ctx.CancellationToken, serializerOptions);
                    }
                }
            }

            // No JSON property — create event-based input enumerator.
            // For retries/reruns, read from the source run's events and copy them to this run (copy-on-read).
            var sourceRunId = ctx.Run.RerunOfRunId ?? ctx.Run.RetryOfRunId;
            var enumerableType = typeof(RunEventInputEnumerable<>).MakeGenericType(itemType);
            return Activator.CreateInstance(enumerableType,
                ctx.RunId, sourceRunId, param.Name!, ctx.Store, ctx.Notifications, ctx.CancellationToken, serializerOptions);
        }

        if (argsJson is { ValueKind: JsonValueKind.Object } jsonObj && serializerOptions is not null)
        {
            foreach (var prop in jsonObj.EnumerateObject())
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
