using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;

namespace Surefire;

internal sealed class HandlerMetadata
{
    public required ParameterInfo[] Parameters { get; init; }

    // (object?[] args) => raw handler return value (Task/ValueTask/T/IAsyncEnumerable/null).
    public required Func<object?[], object?> Invoke { get; init; }

    public required Type ReturnType { get; init; }

    // false for void/Task/ValueTask.
    public required bool HasResult { get; init; }

    // null if return type is not IAsyncEnumerable<T> (or Task/ValueTask wrapping one).
    public Type? AsyncEnumerableElementType { get; init; }

    // null if return type has no result (void/Task/ValueTask).
    public Func<Task, object?>? ExtractTaskResult { get; init; }

    // null if return type is not ValueTask<T>.
    public Func<object, Task>? AsTaskDelegate { get; init; }

    // null if return type is not IAsyncEnumerable<T>.
    public MaterializerDelegate? Materializer { get; init; }

    // Indexed by parameter position. null for parameters that cannot bind from an input stream.
    public required IReadOnlyList<StreamBinderDelegate?> StreamBinders { get; init; }

    [RequiresUnreferencedCode("Reflects over a user-supplied handler delegate.")]
    [RequiresDynamicCode("Reflects over a user-supplied handler delegate.")]
    public static HandlerMetadata Build(Delegate handler)
    {
        var parameters = handler.Method.GetParameters();
        var returnType = handler.Method.ReturnType;

        var invoke = DelegateCompiler.CompileInvoke(handler, parameters);
        var asyncEnumElementType = GetAsyncEnumerableElementType(returnType);
        var hasResult = DetermineHasResult(returnType);
        var extractTaskResult = CompileExtractTaskResult(returnType);
        var asTaskDelegate = DelegateCompiler.CompileAsTask(returnType);
        var materializer = asyncEnumElementType is { } elemType ? BuildMaterializer(elemType) : null;
        var streamBinders = parameters.Select(BuildStreamBinder).ToArray();

        return new()
        {
            Parameters = parameters,
            Invoke = invoke,
            ReturnType = returnType,
            HasResult = hasResult,
            AsyncEnumerableElementType = asyncEnumElementType,
            ExtractTaskResult = extractTaskResult,
            AsTaskDelegate = asTaskDelegate,
            Materializer = materializer,
            StreamBinders = streamBinders
        };
    }

    private static bool DetermineHasResult(Type returnType) =>
        returnType != typeof(void)
        && returnType != typeof(Task)
        && returnType != typeof(ValueTask);

    [RequiresUnreferencedCode("Inspects interfaces of a runtime type which may be trimmed.")]
    private static Type? GetAsyncEnumerableElementType(Type type)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            return type.GetGenericArguments()[0];
        }

        // Handle Task<IAsyncEnumerable<T>> and ValueTask<IAsyncEnumerable<T>>: the handler
        // returns a wrapped async enumerable that AwaitResultAsync unwraps before materializing.
        if (type.IsGenericType)
        {
            var def = type.GetGenericTypeDefinition();
            if (def == typeof(Task<>) || def == typeof(ValueTask<>))
            {
                var inner = type.GetGenericArguments()[0];
                var unwrapped = GetAsyncEnumerableElementType(inner);
                if (unwrapped is { })
                {
                    return unwrapped;
                }
            }
        }

        return type.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
            ?.GetGenericArguments()[0];
    }

    [RequiresUnreferencedCode("Compiles an Expression that materializes a Task<T> result.")]
    [RequiresDynamicCode("Compiles an Expression that materializes a Task<T> result.")]
    private static Func<Task, object?>? CompileExtractTaskResult(Type returnType)
    {
        // Works for both Task<T> and ValueTask<T> returns (after the caller has called AsTask on ValueTask<T>)
        Type? resultType = null;
        if (returnType.IsGenericType)
        {
            var def = returnType.GetGenericTypeDefinition();
            if (def == typeof(Task<>) || def == typeof(ValueTask<>))
            {
                resultType = returnType.GetGenericArguments()[0];
            }
        }

        if (resultType is null)
        {
            return null;
        }

        // Expression: (Task task) => (object?)((Task<T>)task).Result
        var taskParam = Expression.Parameter(typeof(Task), "task");
        var castTask = Expression.Convert(taskParam, typeof(Task<>).MakeGenericType(resultType));
        var result = Expression.Property(castTask, "Result");
        var body = Expression.Convert(result, typeof(object));
        return Expression.Lambda<Func<Task, object?>>(body, taskParam).Compile();
    }

    [RequiresUnreferencedCode("Constructs a generic method instantiation from a runtime element type.")]
    [RequiresDynamicCode("Constructs a generic method instantiation from a runtime element type.")]
    private static MaterializerDelegate BuildMaterializer(Type elementType)
    {
        var method = typeof(HandlerMetadata)
            .GetMethod(nameof(BuildMaterializerCore), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(elementType);
        return (MaterializerDelegate)method.Invoke(null, null)!;
    }

    [RequiresUnreferencedCode("Uses JSON serialization.")]
    [RequiresDynamicCode("Uses JSON serialization.")]
    private static MaterializerDelegate BuildMaterializerCore<T>() =>
        (executor, stream, run, ct) =>
            executor.MaterializeAsyncCore<T>((IAsyncEnumerable<T>)stream, run, ct);

    [RequiresUnreferencedCode("Constructs a generic method instantiation from a runtime element type.")]
    [RequiresDynamicCode("Constructs a generic method instantiation from a runtime element type.")]
    private static StreamBinderDelegate? BuildStreamBinder(ParameterInfo parameter)
    {
        var paramType = parameter.ParameterType;

        if (paramType.IsGenericType && paramType.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            var elementType = paramType.GetGenericArguments()[0];
            var method = typeof(HandlerMetadata)
                .GetMethod(nameof(BuildAsyncEnumerableBinderCore), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(elementType);
            return (StreamBinderDelegate)method.Invoke(null, null)!;
        }

        if (TypeHelpers.TryGetCollectionElementType(paramType, out var collElemType, out var asArray))
        {
            var method = typeof(HandlerMetadata)
                .GetMethod(nameof(BuildCollectionBinderCore), BindingFlags.Static | BindingFlags.NonPublic)!
                .MakeGenericMethod(collElemType);
            return (StreamBinderDelegate)method.Invoke(null, [asArray])!;
        }

        return null;
    }

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private static StreamBinderDelegate BuildAsyncEnumerableBinderCore<T>() =>
        (executor, runId, argName, ct) =>
            Task.FromResult<object?>(executor.CreateInputStreamAsync<T>(runId, argName, ct));

    [RequiresUnreferencedCode("Uses JSON deserialization.")]
    [RequiresDynamicCode("Uses JSON deserialization.")]
    private static StreamBinderDelegate BuildCollectionBinderCore<T>(bool asArray) =>
        async (executor, runId, argName, ct) =>
        {
            var items = new List<T>();
            await foreach (var item in executor.ReadInputStreamAsync<T>(runId, argName, ct))
            {
                items.Add(item);
            }

            return asArray ? items.ToArray() : items;
        };
}

internal delegate Task<List<string>> MaterializerDelegate(
    SurefireExecutorService executor, object stream, JobRun run, CancellationToken ct);

internal delegate Task<object?> StreamBinderDelegate(
    SurefireExecutorService executor, string runId, string argumentName, CancellationToken ct);
