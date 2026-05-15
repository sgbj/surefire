using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Surefire;

/// <summary>
///     Reflection-based fallback that builds <see cref="JobRegistrationDescriptor" /> and
///     <see cref="CallbackDescriptor" /> instances at run time. Used when the Surefire source
///     generator can't intercept a call site (method-group references, dynamic delegates, JIT
///     consumers with the generator disabled). The shape of the produced descriptors matches
///     what the generator emits, so the rest of the runtime is oblivious to which path built them.
/// </summary>
internal static class ReflectionDescriptorBuilder
{
    private const string TrimmingMessage =
        "Building a descriptor by reflecting over a Delegate requires unreferenced code and dynamic " +
        "code generation. Provide a pre-built JobRegistrationDescriptor (e.g. via the Surefire source " +
        "generator) when publishing with Native AOT or trimming.";

    private static readonly MethodInfo ExtractTaskResultOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(ExtractTaskResultCore), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo AsTaskFromValueTaskOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(AsTaskFromValueTaskCore), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo MaterializeOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(MaterializeAsync), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo StreamBinderAsyncEnumerableOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(BindStreamAsAsyncEnumerable), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo StreamBinderListOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(BindStreamAsList), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo StreamBinderArrayOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(BindStreamAsArray), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo InputJsonBinderAsyncEnumerableOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(BindInlineJsonAsAsyncEnumerable), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo InputJsonBinderListOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(BindInlineJsonAsList), BindingFlags.NonPublic | BindingFlags.Static)!;

    private static readonly MethodInfo InputJsonBinderArrayOpen = typeof(ReflectionDescriptorBuilder)
        .GetMethod(nameof(BindInlineJsonAsArray), BindingFlags.NonPublic | BindingFlags.Static)!;

    [RequiresUnreferencedCode(TrimmingMessage)]
    [RequiresDynamicCode(TrimmingMessage)]
    public static JobRegistrationDescriptor BuildJob(string name, Delegate handler)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(handler);

        var method = handler.Method;
        var methodParameters = method.GetParameters();
        var parameters = BuildParameters(methodParameters);
        var (returnKind, asyncElementType, extractTaskResult, asTask) = ClassifyReturn(method.ReturnType);
        var invoke = CompileInvoker(handler.GetType(), methodParameters, method.ReturnType);

        return new()
        {
            Name = name,
            Handler = handler,
            Parameters = parameters,
            Invoke = invoke,
            ReturnKind = returnKind,
            ReturnType = method.ReturnType,
            AsyncEnumerableElementType = asyncElementType,
            ExtractTaskResult = extractTaskResult,
            AsTask = asTask,
            Materializer = asyncElementType is { } e ? CreateMaterializer(e) : null,
            StreamBinders = BuildStreamBinders(parameters),
            InputJsonBinders = BuildInputJsonBinders(parameters),
            ParameterJsonTypeInfoFactories = BuildParameterJsonFactories(parameters),
            ResultJsonTypeInfoFactory = BuildResultJsonFactory(returnKind, method.ReturnType, asyncElementType),
            OutputStreamElementJsonTypeInfoFactory = asyncElementType is { } streamElement
                ? opts => opts.GetTypeInfo(streamElement)
                : null,
            StreamParameterJsonTypeInfoFactories = BuildStreamParameterJsonFactories(parameters)
        };
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    [RequiresDynamicCode(TrimmingMessage)]
    public static CallbackDescriptor BuildCallback(Delegate handler)
    {
        ArgumentNullException.ThrowIfNull(handler);

        var method = handler.Method;
        var methodParameters = method.GetParameters();
        var parameters = BuildParameters(methodParameters);
        var (returnKind, _, _, asTask) = ClassifyReturn(method.ReturnType);
        var invoke = CompileInvoker(handler.GetType(), methodParameters, method.ReturnType);

        return new()
        {
            Handler = handler,
            Parameters = parameters,
            Invoke = invoke,
            ReturnKind = returnKind,
            AsTask = asTask
        };
    }

    /// <summary>
    ///     Compiles an Expression tree that mirrors the generator's <c>(args, h) => ((TDelegate)h)(...)</c>
    ///     invocation shim: cast each argument to its declared parameter type, invoke the typed
    ///     delegate, and box the result (or return <c>null</c> for void). Exceptions thrown by the
    ///     user's handler propagate as their original type, not wrapped in TargetInvocationException.
    /// </summary>
    [RequiresDynamicCode(TrimmingMessage)]
    private static Func<object?[], Delegate, object?> CompileInvoker(
        Type delegateType, ParameterInfo[] parameters, Type returnType)
    {
        var argsParam = Expression.Parameter(typeof(object?[]), "args");
        var handlerParam = Expression.Parameter(typeof(Delegate), "h");
        var typedHandler = Expression.Convert(handlerParam, delegateType);

        var typedArgs = new Expression[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            var indexed = Expression.ArrayIndex(argsParam, Expression.Constant(i));
            typedArgs[i] = Expression.Convert(indexed, parameters[i].ParameterType);
        }

        var call = Expression.Invoke(typedHandler, typedArgs);
        Expression body = returnType == typeof(void)
            ? Expression.Block(call, Expression.Constant(null, typeof(object)))
            : Expression.Convert(call, typeof(object));

        return Expression.Lambda<Func<object?[], Delegate, object?>>(body, argsParam, handlerParam).Compile();
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    private static ParameterDescriptor[] BuildParameters(ParameterInfo[] parameters)
    {
        var nullability = new NullabilityInfoContext();
        var result = new ParameterDescriptor[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            var p = parameters[i];
            var type = p.ParameterType;
            var (kind, streamElement, streamShape) = ClassifyParameter(type);
            result[i] = new(
                p.Name ?? $"arg{i}",
                type,
                kind,
                p.HasDefaultValue,
                p.HasDefaultValue ? p.DefaultValue : null,
                IsNullable(p, type, nullability),
                streamElement,
                streamShape);
        }

        return result;
    }

    private static bool IsNullable(ParameterInfo parameter, Type type, NullabilityInfoContext nullability)
    {
        if (type.IsValueType)
        {
            return Nullable.GetUnderlyingType(type) is { };
        }

        // Reference type: read the nullable annotation. WriteState reflects the declared
        // annotation at the parameter position (the callee's "write" of the value).
        return nullability.Create(parameter).WriteState != NullabilityState.NotNull;
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    private static (ParameterKind Kind, Type? StreamElement, StreamShape? StreamShape) ClassifyParameter(Type type)
    {
        if (type == typeof(JobContext))
        {
            return (ParameterKind.JobContext, null, null);
        }

        if (type == typeof(CancellationToken))
        {
            return (ParameterKind.CancellationToken, null, null);
        }

        if (type == typeof(IServiceProvider))
        {
            return (ParameterKind.ServiceProvider, null, null);
        }

        // Stream-shaped parameters bind from a declared input stream OR from inline JSON.
        var (element, shape) = ResolveStreamShape(type);
        if (element is { })
        {
            return (ParameterKind.Stream, element, shape);
        }

        // Mirrors the generator: defer DI vs JSON to IServiceProviderIsService at registration time.
        return (ParameterKind.ServiceOrJson, null, null);
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    private static (Type? Element, StreamShape? Shape) ResolveStreamShape(Type type)
    {
        if (type.IsArray && type.GetArrayRank() == 1)
        {
            return (type.GetElementType(), StreamShape.Array);
        }

        if (type.IsGenericType)
        {
            var definition = type.GetGenericTypeDefinition();
            var element = type.GetGenericArguments()[0];
            if (definition == typeof(IAsyncEnumerable<>))
            {
                return (element, StreamShape.AsyncEnumerable);
            }

            if (definition == typeof(List<>)
                || definition == typeof(IReadOnlyList<>)
                || definition == typeof(IList<>)
                || definition == typeof(IEnumerable<>))
            {
                return (element, StreamShape.List);
            }
        }

        // Concrete types implementing IAsyncEnumerable<T> (e.g. ChannelReader-style wrappers).
        return TryGetAsyncEnumerableElement(type) is { } ifaceElement
            ? (ifaceElement, StreamShape.AsyncEnumerable)
            : (null, null);
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    [RequiresDynamicCode(TrimmingMessage)]
    private static (ReturnKind Kind, Type? AsyncElement, Func<Task, object?>? ExtractTaskResult, Func<object, Task>?
        AsTask)
        ClassifyReturn(Type returnType)
    {
        if (returnType == typeof(void))
        {
            return (ReturnKind.Void, null, null, null);
        }

        if (returnType == typeof(Task))
        {
            return (ReturnKind.Task, null, null, null);
        }

        if (returnType == typeof(ValueTask))
        {
            return (ReturnKind.ValueTask, null, null, null);
        }

        if (returnType.IsGenericType)
        {
            var definition = returnType.GetGenericTypeDefinition();
            var argument = returnType.GetGenericArguments()[0];

            if (definition == typeof(Task<>))
            {
                if (TryGetAsyncEnumerableElement(argument) is { } innerElement)
                {
                    return (ReturnKind.AsyncEnumerable, innerElement, null, null);
                }

                return (ReturnKind.TaskOfT, null, CreateExtractTaskResult(argument), null);
            }

            if (definition == typeof(ValueTask<>))
            {
                if (TryGetAsyncEnumerableElement(argument) is { } innerElement)
                {
                    return (ReturnKind.AsyncEnumerable, innerElement, null, CreateAsTaskFromValueTask(argument));
                }

                return (ReturnKind.ValueTaskOfT, null, CreateExtractTaskResult(argument),
                    CreateAsTaskFromValueTask(argument));
            }

            if (definition == typeof(IAsyncEnumerable<>))
            {
                return (ReturnKind.AsyncEnumerable, argument, null, null);
            }
        }

        return (ReturnKind.Scalar, null, null, null);
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    private static Type? TryGetAsyncEnumerableElement(Type type)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
        {
            return type.GetGenericArguments()[0];
        }

        // Walk implemented interfaces so concrete types like Channel<T>.ReadAllAsync()'s return
        // type or user-authored IAsyncEnumerable<T> implementations are recognized.
        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IAsyncEnumerable<>))
            {
                return iface.GetGenericArguments()[0];
            }
        }

        return null;
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    [RequiresDynamicCode(TrimmingMessage)]
    private static Func<Task, object?> CreateExtractTaskResult(Type resultType) =>
        (Func<Task, object?>)ExtractTaskResultOpen.MakeGenericMethod(resultType)
            .CreateDelegate(typeof(Func<Task, object?>));

    [RequiresUnreferencedCode(TrimmingMessage)]
    [RequiresDynamicCode(TrimmingMessage)]
    private static Func<object, Task> CreateAsTaskFromValueTask(Type resultType) =>
        (Func<object, Task>)AsTaskFromValueTaskOpen.MakeGenericMethod(resultType)
            .CreateDelegate(typeof(Func<object, Task>));

    [RequiresUnreferencedCode(TrimmingMessage)]
    [RequiresDynamicCode(TrimmingMessage)]
    private static OutputStreamMaterializer CreateMaterializer(Type elementType)
    {
        var closed = MaterializeOpen.MakeGenericMethod(elementType);
        return (OutputStreamMaterializer)closed.CreateDelegate(typeof(OutputStreamMaterializer));
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    [RequiresDynamicCode(TrimmingMessage)]
    private static InputStreamBinder?[] BuildStreamBinders(ParameterDescriptor[] parameters)
    {
        var binders = new InputStreamBinder?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            if (parameters[i] is { Kind: ParameterKind.Stream, StreamElementType: { } element, StreamShape: { } shape })
            {
                var open = shape switch
                {
                    StreamShape.AsyncEnumerable => StreamBinderAsyncEnumerableOpen,
                    StreamShape.List => StreamBinderListOpen,
                    StreamShape.Array => StreamBinderArrayOpen,
                    _ => throw new InvalidOperationException($"Unsupported stream shape {shape}.")
                };
                binders[i] =
                    (InputStreamBinder)open.MakeGenericMethod(element).CreateDelegate(typeof(InputStreamBinder));
            }
        }

        return binders;
    }

    [RequiresUnreferencedCode(TrimmingMessage)]
    [RequiresDynamicCode(TrimmingMessage)]
    private static Func<JsonElement, JsonSerializerOptions, object?>?[] BuildInputJsonBinders(
        ParameterDescriptor[] parameters)
    {
        var binders = new Func<JsonElement, JsonSerializerOptions, object?>?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            if (parameters[i] is { Kind: ParameterKind.Stream, StreamElementType: { } element, StreamShape: { } shape })
            {
                var open = shape switch
                {
                    StreamShape.AsyncEnumerable => InputJsonBinderAsyncEnumerableOpen,
                    StreamShape.List => InputJsonBinderListOpen,
                    StreamShape.Array => InputJsonBinderArrayOpen,
                    _ => throw new InvalidOperationException($"Unsupported stream shape {shape}.")
                };
                binders[i] = (Func<JsonElement, JsonSerializerOptions, object?>)open
                    .MakeGenericMethod(element)
                    .CreateDelegate(typeof(Func<JsonElement, JsonSerializerOptions, object?>));
            }
        }

        return binders;
    }

    private static JsonTypeInfoFactory?[] BuildParameterJsonFactories(ParameterDescriptor[] parameters)
    {
        var factories = new JsonTypeInfoFactory?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            var p = parameters[i];
            if (p.Kind is ParameterKind.Json or ParameterKind.ServiceOrJson)
            {
                var type = p.Type;
                factories[i] = opts => opts.GetTypeInfo(type);
            }
        }

        return factories;
    }

    private static JsonTypeInfoFactory? BuildResultJsonFactory(
        ReturnKind returnKind, Type returnType, Type? asyncElementType)
    {
        var resultType = returnKind switch
        {
            ReturnKind.Void or ReturnKind.Task or ReturnKind.ValueTask => null,
            ReturnKind.TaskOfT or ReturnKind.ValueTaskOfT => returnType.GetGenericArguments()[0],
            ReturnKind.AsyncEnumerable => asyncElementType,
            _ => returnType
        };

        return resultType is null ? null : opts => opts.GetTypeInfo(resultType);
    }

    private static JsonTypeInfoFactory?[] BuildStreamParameterJsonFactories(ParameterDescriptor[] parameters)
    {
        var factories = new JsonTypeInfoFactory?[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
        {
            if (parameters[i] is { Kind: ParameterKind.Stream, StreamElementType: { } element })
            {
                factories[i] = opts => opts.GetTypeInfo(element);
            }
        }

        return factories;
    }

    // Generic helpers reached via MakeGenericMethod. Each matches the shape of its delegate target
    // exactly (no overloads, no params adjustments) so CreateDelegate doesn't need adapters.

    private static object? ExtractTaskResultCore<T>(Task task) => ((Task<T>)task).Result;

    private static Task AsTaskFromValueTaskCore<T>(object valueTask) => ((ValueTask<T>)valueTask).AsTask();

    private static Task<IReadOnlyList<string>> MaterializeAsync<T>(
        IJobRunPipe pipe, object stream, JsonTypeInfo typeInfo, JobRun run, CancellationToken cancellationToken)
        => pipe.WriteOutputStreamAsync((IAsyncEnumerable<T>)stream, (JsonTypeInfo<T>)typeInfo, run, cancellationToken);

    private static Task<object?> BindStreamAsAsyncEnumerable<T>(
        IJobRunPipe pipe, JobRun run, string argumentName, JsonTypeInfo typeInfo, CancellationToken cancellationToken)
        => Task.FromResult<object?>(
            pipe.ReadInputStreamAsync(run, argumentName, (JsonTypeInfo<T>)typeInfo, cancellationToken));

    private static async Task<object?> BindStreamAsList<T>(
        IJobRunPipe pipe, JobRun run, string argumentName, JsonTypeInfo typeInfo, CancellationToken cancellationToken)
    {
        var ti = (JsonTypeInfo<T>)typeInfo;
        var list = new List<T>();
        await foreach (var item in pipe.ReadInputStreamAsync(run, argumentName, ti, cancellationToken))
        {
            list.Add(item);
        }

        return list;
    }

    private static async Task<object?> BindStreamAsArray<T>(
        IJobRunPipe pipe, JobRun run, string argumentName, JsonTypeInfo typeInfo, CancellationToken cancellationToken)
    {
        var ti = (JsonTypeInfo<T>)typeInfo;
        var list = new List<T>();
        await foreach (var item in pipe.ReadInputStreamAsync(run, argumentName, ti, cancellationToken))
        {
            list.Add(item);
        }

        return list.ToArray();
    }

    private static object? BindInlineJsonAsAsyncEnumerable<T>(JsonElement value, JsonSerializerOptions options)
    {
        var typeInfo = (JsonTypeInfo<List<T>>)options.GetTypeInfo(typeof(List<T>));
        var list = JsonSerializer.Deserialize(value.GetRawText(), typeInfo) ?? [];
        return YieldAsync(list);

        static async IAsyncEnumerable<T> YieldAsync(List<T> source)
        {
            foreach (var item in source)
            {
                yield return item;
                await Task.Yield();
            }
        }
    }

    private static object? BindInlineJsonAsList<T>(JsonElement value, JsonSerializerOptions options)
    {
        var typeInfo = (JsonTypeInfo<List<T>>)options.GetTypeInfo(typeof(List<T>));
        return JsonSerializer.Deserialize(value.GetRawText(), typeInfo);
    }

    private static object? BindInlineJsonAsArray<T>(JsonElement value, JsonSerializerOptions options)
    {
        var typeInfo = (JsonTypeInfo<T[]>)options.GetTypeInfo(typeof(T[]));
        return JsonSerializer.Deserialize(value.GetRawText(), typeInfo);
    }
}
