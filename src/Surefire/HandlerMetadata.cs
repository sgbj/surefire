using System.Linq.Expressions;
using System.Reflection;

namespace Surefire;

internal sealed class HandlerMetadata
{
    // Avoids GetParameters() per invocation
    public required ParameterInfo[] Parameters { get; init; }

    // Replaces DynamicInvoke — compiled expression tree
    // (object?[] args) => raw handler return value (Task/ValueTask/T/IAsyncEnumerable/null)
    public required Func<object?[], object?> Invoke { get; init; }

    // Return type info
    public required Type ReturnType { get; init; }
    public required bool HasResult { get; init; }  // false for void/Task/ValueTask

    // null if output type is not IAsyncEnumerable<T>
    public Type? AsyncEnumerableElementType { get; init; }

    // Compiled Task<T>.Result extractor — replaces GetProperty("Result").GetValue()
    // null if return type has no result (void/Task/ValueTask)
    public Func<Task, object?>? ExtractTaskResult { get; init; }

    // Compiled ValueTask<T>.AsTask() — replaces per-call reflection for ValueTask<T> returns
    // null if return type is not ValueTask<T>
    public Func<object, Task>? AsTaskDelegate { get; init; }

    // Compiled output stream materializer — replaces MakeGenericMethod per call in MaterializeAsyncEnumerableAsync
    // null if output is not IAsyncEnumerable<T>
    public MaterializerDelegate? Materializer { get; init; }

    // Per-parameter input stream binders (indexed by parameter position) — replaces MakeGenericMethod per call in BindStreamInputParameterAsync
    // null entries for parameters that cannot bind from input stream
    public required IReadOnlyList<StreamBinderDelegate?> StreamBinders { get; init; }

    public static HandlerMetadata Build(Delegate handler)
    {
        var parameters = handler.Method.GetParameters();
        var returnType = handler.Method.ReturnType;

        var invoke = CompileInvoke(handler, parameters);
        var asyncEnumElementType = GetAsyncEnumerableElementType(returnType);
        var hasResult = DetermineHasResult(returnType);
        var extractTaskResult = CompileExtractTaskResult(returnType);
        var asTaskDelegate = CompileAsTask(returnType);
        var materializer = asyncEnumElementType is { } elemType ? BuildMaterializer(elemType) : null;
        var streamBinders = parameters.Select(BuildStreamBinder).ToArray();

        return new HandlerMetadata
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

    private static Func<object?[], object?> CompileInvoke(Delegate handler, ParameterInfo[] parameters)
    {
        var argsParam = Expression.Parameter(typeof(object?[]), "args");
        var callArgs = parameters.Select((p, i) =>
            (Expression)Expression.Convert(
                Expression.ArrayIndex(argsParam, Expression.Constant(i)),
                p.ParameterType)).ToArray();
        var call = Expression.Invoke(Expression.Constant(handler), callArgs);
        Expression body = handler.Method.ReturnType == typeof(void)
            ? Expression.Block(typeof(object), call, Expression.Constant(null, typeof(object)))
            : Expression.Convert(call, typeof(object));
        return Expression.Lambda<Func<object?[], object?>>(body, argsParam).Compile();
    }

    private static Type? GetAsyncEnumerableElementType(Type type)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
            return type.GetGenericArguments()[0];
        return type.GetInterfaces()
            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
            ?.GetGenericArguments()[0];
    }

    private static Func<Task, object?>? CompileExtractTaskResult(Type returnType)
    {
        // Works for both Task<T> and ValueTask<T> returns (after the caller has called AsTask on ValueTask<T>)
        Type? resultType = null;
        if (returnType.IsGenericType)
        {
            var def = returnType.GetGenericTypeDefinition();
            if (def == typeof(Task<>) || def == typeof(ValueTask<>))
                resultType = returnType.GetGenericArguments()[0];
        }
        if (resultType is null) return null;

        // Expression: (Task task) => (object?)((Task<T>)task).Result
        var taskParam = Expression.Parameter(typeof(Task), "task");
        var castTask = Expression.Convert(taskParam, typeof(Task<>).MakeGenericType(resultType));
        var result = Expression.Property(castTask, "Result");
        var body = Expression.Convert(result, typeof(object));
        return Expression.Lambda<Func<Task, object?>>(body, taskParam).Compile();
    }

    private static Func<object, Task>? CompileAsTask(Type returnType)
    {
        if (!returnType.IsGenericType || returnType.GetGenericTypeDefinition() != typeof(ValueTask<>))
            return null;
        var resultType = returnType.GetGenericArguments()[0];
        // Expression: (object vt) => ((ValueTask<T>)vt).AsTask()
        var vtParam = Expression.Parameter(typeof(object), "vt");
        var unboxed = Expression.Convert(vtParam, typeof(ValueTask<>).MakeGenericType(resultType));
        var call = Expression.Call(unboxed, typeof(ValueTask<>).MakeGenericType(resultType).GetMethod("AsTask")!);
        var body = Expression.Convert(call, typeof(Task));
        return Expression.Lambda<Func<object, Task>>(body, vtParam).Compile();
    }

    private static MaterializerDelegate BuildMaterializer(Type elementType)
    {
        var method = typeof(HandlerMetadata)
            .GetMethod(nameof(BuildMaterializerCore), BindingFlags.Static | BindingFlags.NonPublic)!
            .MakeGenericMethod(elementType);
        return (MaterializerDelegate)method.Invoke(null, null)!;
    }

    private static MaterializerDelegate BuildMaterializerCore<T>() =>
        (executor, stream, run, ct) =>
            executor.MaterializeCoreAsync<T>((IAsyncEnumerable<T>)stream, run, ct);

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

    private static StreamBinderDelegate BuildAsyncEnumerableBinderCore<T>() =>
        (executor, runId, argName, ct) =>
            Task.FromResult<object?>(executor.CreateInputStreamAsync<T>(runId, argName, ct));

    private static StreamBinderDelegate BuildCollectionBinderCore<T>(bool asArray) =>
        async (executor, runId, argName, ct) =>
        {
            var items = new List<T>();
            await foreach (var item in executor.ReadInputStreamAsync<T>(runId, argName, ct))
                items.Add(item);
            return asArray ? (object?)items.ToArray() : items;
        };

}

internal delegate Task<List<string>> MaterializerDelegate(
    SurefireExecutorService executor, object stream, JobRun run, CancellationToken ct);

internal delegate Task<object?> StreamBinderDelegate(
    SurefireExecutorService executor, string runId, string argumentName, CancellationToken ct);
