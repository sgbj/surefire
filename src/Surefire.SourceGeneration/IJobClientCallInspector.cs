using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Surefire.SourceGeneration;

/// <summary>
///     Maps the names of <see cref="Surefire.IJobClient" /> methods we intercept and the name
///     of their <c>args</c> parameter (if any). That name is used to find the args argument by
///     name or positionally on the call site syntax.
/// </summary>
internal static class IJobClientCallInspector
{
    private const string JobClientInterfaceFullName = "Surefire.IJobClient";
    private const string BatchItemFullName = "Surefire.BatchItem";
    private const string RunArgumentsFullName = "Surefire.RunArguments";

    /// <summary>
    ///     Methods the generator can intercept and the name of their args parameter. <c>HasResultT</c>
    ///     is true when the method is generic and serializes a result type. <c>HasResultT</c> is false
    ///     for the non-generic <c>RunBatchAsync</c> entry below because we never deserialize a result,
    ///     even though we still need to map <c>args</c> for AOT.
    /// </summary>
    private static readonly Dictionary<string, (IJobClientMethod Method, string? ArgsParam, bool HasResultT)> Catalog =
        new()
        {
            ["TriggerAsync"] = (IJobClientMethod.TriggerAsync, "args", false),
            ["RunAsync"] = (IJobClientMethod.RunAsync, "args", true),
            ["StreamAsync"] = (IJobClientMethod.StreamAsync, "args", true),
            ["WaitEachAsync"] = (IJobClientMethod.WaitEachAsync, null, true),
            ["RunBatchAsync"] = (IJobClientMethod.RunBatchAsync, "args", true),
            ["StreamBatchAsync"] = (IJobClientMethod.StreamBatchAsync, "args", true),
            ["TriggerBatchAsync"] = (IJobClientMethod.TriggerBatchAsync, "args", false),
            ["Create"] = (IJobClientMethod.BatchItemCreate, "args", false)
        };

    public static bool IsCandidate(string methodName) => Catalog.ContainsKey(methodName);

    private static bool IsKnownReceiver(IMethodSymbol method, IJobClientMethod kind)
    {
        var containing = method.ContainingType;
        if (containing is null)
        {
            return false;
        }

        if (kind is IJobClientMethod.BatchItemCreate)
        {
            return containing.ToDisplayString() == BatchItemFullName;
        }

        if (containing.ToDisplayString() == JobClientInterfaceFullName)
        {
            return true;
        }

        foreach (var iface in containing.AllInterfaces)
        {
            if (iface.ToDisplayString() == JobClientInterfaceFullName)
            {
                return true;
            }
        }

        return false;
    }

    public static IJobClientCall? Inspect(
        InvocationExpressionSyntax invocation,
        SemanticModel semanticModel,
        IMethodSymbol method,
        CancellationToken cancellationToken,
        bool requireInterceptableLocation = true)
    {
        if (!Catalog.TryGetValue(method.Name, out var entry))
        {
            return null;
        }

        // Accept interface calls, concrete implementations of IJobClient, and BatchItem.Create.
        if (!IsKnownReceiver(method, entry.Method))
        {
            return null;
        }

        // Skip generic-only callers when the user invoked a non-generic overload that doesn't
        // need a TResult. RunAsync and RunBatchAsync are the exceptions: each has a non-generic
        // AOT-safe overload we want to forward to, so the emitter branches by ResultTypeName
        // instead.
        if (entry.HasResultT && method.TypeArguments.Length == 0
                             && entry.Method is not IJobClientMethod.RunAsync
                             && entry.Method is not IJobClientMethod.RunBatchAsync)
        {
            return null;
        }

        // Don't re-intercept the AOT-safe overloads that already take RunArguments / IEnumerable<RunArguments?>.
        if (UsesAotSafeShape(method))
        {
            return null;
        }

        var interceptsAttribute = string.Empty;
        if (requireInterceptableLocation)
        {
            var location = semanticModel.GetInterceptableLocation(invocation, cancellationToken);
            if (location is null)
            {
                return null;
            }

            interceptsAttribute = location.GetInterceptsLocationAttributeSyntax();
        }

        string? resultTypeName = null;
        var resultIsAsyncEnumerable = false;
        string? resultElementTypeName = null;
        if (entry.HasResultT && method.TypeArguments.Length > 0)
        {
            var resultTypeSymbol = method.TypeArguments[0];

            // T = object/dynamic: no useful static serialization. Skip interception; the call
            // falls through to the [RequiresUnreferencedCode]-attributed overload, which surfaces
            // the standard IL2026/IL3050 warning under AOT.
            if (resultTypeSymbol.SpecialType == SpecialType.System_Object
                || resultTypeSymbol.TypeKind == TypeKind.Dynamic)
            {
                return null;
            }

            resultTypeName = resultTypeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

            if (resultTypeSymbol is INamedTypeSymbol nt && nt.IsGenericType
                                                        && nt.ConstructedFrom.ToDisplayString() ==
                                                        "System.Collections.Generic.IAsyncEnumerable<T>")
            {
                resultIsAsyncEnumerable = true;
                resultElementTypeName = nt.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            }
        }

        var receiverTypeName = method.ContainingType!.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

        if (entry.ArgsParam is null)
        {
            // WaitEachAsync<T> only needs interception when T is IAsyncEnumerable<U>; otherwise the
            // AOT-safe overload on the client handles the simple result-type case directly.
            if (entry.Method is IJobClientMethod.WaitEachAsync && !resultIsAsyncEnumerable)
            {
                return null;
            }

            return new(interceptsAttribute, entry.Method, receiverTypeName, resultTypeName,
                resultIsAsyncEnumerable, resultElementTypeName, ArgsExpressionShape.None,
                EquatableArray<AnonArgProperty>.Empty);
        }

        var argsExpr = ResolveArgument(invocation, entry.ArgsParam, method);

        if (IsBatchMethod(entry.Method))
        {
            return InspectBatchArgs(argsExpr, semanticModel, interceptsAttribute, entry.Method, receiverTypeName,
                resultTypeName, resultIsAsyncEnumerable, resultElementTypeName, cancellationToken);
        }

        return InspectArgs(argsExpr, semanticModel, interceptsAttribute, entry.Method, receiverTypeName,
            resultTypeName, resultIsAsyncEnumerable, resultElementTypeName, cancellationToken);
    }

    private static bool IsBatchMethod(IJobClientMethod method) => method
        is IJobClientMethod.TriggerBatchAsync
        or IJobClientMethod.RunBatchAsync
        or IJobClientMethod.StreamBatchAsync;

    private static bool UsesAotSafeShape(IMethodSymbol method)
    {
        // The AOT-safe overloads have either a RunArguments/IEnumerable<RunArguments?> parameter
        // OR a JsonTypeInfo<T> parameter. Either marker is enough.
        foreach (var p in method.Parameters)
        {
            var paramFqn = p.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (paramFqn == "global::" + RunArgumentsFullName
                || paramFqn == "global::Surefire.RunArguments?")
            {
                return true;
            }

            if (paramFqn.StartsWith("global::System.Collections.Generic.IEnumerable<global::Surefire.RunArguments",
                    StringComparison.Ordinal)
                || paramFqn.StartsWith("global::System.Collections.Generic.IEnumerable<Surefire.RunArguments",
                    StringComparison.Ordinal))
            {
                return true;
            }

            if (paramFqn.StartsWith("global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<",
                    StringComparison.Ordinal)
                || paramFqn.StartsWith("System.Text.Json.Serialization.Metadata.JsonTypeInfo<",
                    StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

    private static ExpressionSyntax? ResolveArgument(InvocationExpressionSyntax invocation, string parameterName,
        IMethodSymbol method)
    {
        // Find named argument first.
        foreach (var arg in invocation.ArgumentList.Arguments)
        {
            if (arg.NameColon is { Name.Identifier.ValueText: { } n } && n == parameterName)
            {
                return arg.Expression;
            }
        }

        // Fall back to positional. Find the position of the parameter on the method symbol.
        var position = -1;
        for (var i = 0; i < method.Parameters.Length; i++)
        {
            if (method.Parameters[i].Name == parameterName)
            {
                position = i;
                break;
            }
        }

        if (position < 0 || position >= invocation.ArgumentList.Arguments.Count)
        {
            return null;
        }

        var candidate = invocation.ArgumentList.Arguments[position];
        // Skip if this slot is a name-colon-bound argument for a different parameter.
        if (candidate.NameColon is { } nc && nc.Name.Identifier.ValueText != parameterName)
        {
            return null;
        }

        return candidate.Expression;
    }

    private static IJobClientCall? InspectBatchArgs(
        ExpressionSyntax? argsExpr,
        SemanticModel semanticModel,
        string interceptsAttribute,
        IJobClientMethod method,
        string receiverTypeName,
        string? resultTypeName,
        bool resultIsAsyncEnumerable,
        string? resultElementTypeName,
        CancellationToken cancellationToken)
    {
        // A literal-null batch enumerable can't be intercepted usefully: the AOT-safe overload
        // would still NRE on enumeration. Let the call fall through to the [Requires*] path.
        if (argsExpr is null
            || (argsExpr is LiteralExpressionSyntax literal && literal.Token.IsKind(SyntaxKind.NullKeyword)))
        {
            return null;
        }

        var typeInfo = semanticModel.GetTypeInfo(argsExpr, cancellationToken);
        var type = typeInfo.Type ?? typeInfo.ConvertedType;
        if (type is null)
        {
            return null;
        }

        var element = GetEnumerableElementType(type);
        if (element is null)
        {
            return null;
        }

        if (element.IsAnonymousType)
        {
            var props = InspectAnonymousType((INamedTypeSymbol)element);
            return new(interceptsAttribute, method, receiverTypeName, resultTypeName,
                resultIsAsyncEnumerable, resultElementTypeName, ArgsExpressionShape.Anonymous, props);
        }

        if (!IsStaticallyResolvable(element) || TypeAccessibility.IsInaccessibleFromGenerated(element))
        {
            return null;
        }

        var elementFqn = element.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        // RunArguments enumerables already bind to the AOT-safe overload via variance; if one
        // slips through (e.g., explicit cast), let it fall through to the [Requires*] overload.
        if (elementFqn == "global::" + RunArgumentsFullName || elementFqn == "global::Surefire.RunArguments?")
        {
            return null;
        }

        return new(interceptsAttribute, method, receiverTypeName, resultTypeName,
            resultIsAsyncEnumerable, resultElementTypeName, ArgsExpressionShape.NamedType,
            EquatableArray<AnonArgProperty>.Empty, elementFqn);
    }

    /// <summary>
    ///     A type is statically resolvable for source-generated serialization when its identity is
    ///     known at compile time. <c>object</c>, <c>dynamic</c>, generic type parameters, and
    ///     pointer types all collapse the static type, so we fall back to the reflective path.
    /// </summary>
    private static bool IsStaticallyResolvable(ITypeSymbol type) =>
        type.SpecialType != SpecialType.System_Object
        && type.TypeKind != TypeKind.Dynamic
        && type.TypeKind != TypeKind.TypeParameter
        && type.TypeKind != TypeKind.Pointer;

    private static ITypeSymbol? GetEnumerableElementType(ITypeSymbol type)
    {
        if (type is IArrayTypeSymbol arr)
        {
            return arr.ElementType;
        }

        if (type is INamedTypeSymbol named)
        {
            if (named.IsGenericType
                && named.ConstructedFrom.ToDisplayString() == "System.Collections.Generic.IEnumerable<T>")
            {
                return named.TypeArguments[0];
            }

            foreach (var iface in named.AllInterfaces)
            {
                if (iface.IsGenericType
                    && iface.ConstructedFrom.ToDisplayString() == "System.Collections.Generic.IEnumerable<T>")
                {
                    return iface.TypeArguments[0];
                }
            }
        }

        return null;
    }

    private static IJobClientCall? InspectArgs(
        ExpressionSyntax? argsExpr,
        SemanticModel semanticModel,
        string interceptsAttribute,
        IJobClientMethod method,
        string receiverTypeName,
        string? resultTypeName,
        bool resultIsAsyncEnumerable,
        string? resultElementTypeName,
        CancellationToken cancellationToken)
    {
        if (argsExpr is null
            || (argsExpr is LiteralExpressionSyntax literal && literal.Token.IsKind(SyntaxKind.NullKeyword)))
        {
            return new(interceptsAttribute, method, receiverTypeName, resultTypeName, resultIsAsyncEnumerable,
                resultElementTypeName, ArgsExpressionShape.Null, EquatableArray<AnonArgProperty>.Empty);
        }

        var typeInfo = semanticModel.GetTypeInfo(argsExpr, cancellationToken);
        var type = typeInfo.Type ?? typeInfo.ConvertedType;
        if (type is null)
        {
            return null;
        }

        if (type.IsAnonymousType)
        {
            var props = InspectAnonymousType((INamedTypeSymbol)type);
            return new(interceptsAttribute, method, receiverTypeName, resultTypeName, resultIsAsyncEnumerable,
                resultElementTypeName, ArgsExpressionShape.Anonymous, props);
        }

        var typeFqn = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        if (typeFqn == "global::" + RunArgumentsFullName || typeFqn == "global::Surefire.RunArguments?")
        {
            return new(interceptsAttribute, method, receiverTypeName, resultTypeName, resultIsAsyncEnumerable,
                resultElementTypeName, ArgsExpressionShape.RunArguments,
                EquatableArray<AnonArgProperty>.Empty);
        }

        if (!IsStaticallyResolvable(type) || TypeAccessibility.IsInaccessibleFromGenerated(type))
        {
            // object/dynamic/type-parameter erases the static identity; private/file-scoped types
            // can't be named from Surefire.Generated. Either way, let the reflective path handle it.
            return null;
        }

        // Named class/record/struct: serialize the whole instance via the runtime's JsonTypeInfo<T>.
        // This honors [JsonPropertyName], naming policies, and other JsonSerializerContext settings
        // that per-property emission would silently bypass.
        return new(interceptsAttribute, method, receiverTypeName, resultTypeName, resultIsAsyncEnumerable,
            resultElementTypeName, ArgsExpressionShape.NamedType,
            EquatableArray<AnonArgProperty>.Empty, typeFqn);
    }

    private static ImmutableArray<AnonArgProperty> InspectAnonymousType(INamedTypeSymbol anonType)
    {
        var builder = ImmutableArray.CreateBuilder<AnonArgProperty>();
        foreach (var member in anonType.GetMembers())
        {
            if (member is not IPropertySymbol prop)
            {
                continue;
            }

            var propType = prop.Type;
            var (isStream, streamElement) = ClassifyStreamShape(propType);
            builder.Add(new(
                prop.Name,
                propType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                isStream,
                streamElement));
        }

        return builder.ToImmutable();
    }

    private static (bool IsStream, string? Element) ClassifyStreamShape(ITypeSymbol type)
    {
        // Only true async streams qualify as stream args. Collections (List<T>, T[], etc.) are
        // serialized inline like any other JSON value.
        if (type is INamedTypeSymbol named && named.IsGenericType
                                           && named.ConstructedFrom.ToDisplayString() ==
                                           "System.Collections.Generic.IAsyncEnumerable<T>")
        {
            var element = named.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            return (true, element);
        }

        return (false, null);
    }
}
