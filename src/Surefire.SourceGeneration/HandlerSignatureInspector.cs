using System.Collections.Immutable;
using System.Globalization;
using Microsoft.CodeAnalysis;

namespace Surefire.SourceGeneration;

internal static class HandlerSignatureInspector
{
    private const string JobContextFullName = "Surefire.JobContext";
    private const string CancellationTokenFullName = "System.Threading.CancellationToken";
    private const string ServiceProviderFullName = "System.IServiceProvider";

    /// <summary>
    ///     Returns a <see cref="HandlerSignature" /> for handlers the generator can emit a typed
    ///     descriptor for, or <c>null</c> when the signature contains anonymous types, inaccessible
    ///     types, or <c>ref</c>/<c>in</c>/<c>out</c> parameters. The call site then falls through
    ///     to the <c>[RequiresUnreferencedCode]</c>-annotated overload, which produces the
    ///     standard IL2026/IL3050 warning under AOT.
    /// </summary>
    public static HandlerSignature? Inspect(IMethodSymbol handler)
    {
        if (ContainsAnonymousType(handler.ReturnType))
        {
            return null;
        }

        var parameters = new List<HandlerParameter>(handler.Parameters.Length);
        foreach (var parameter in handler.Parameters)
        {
            if (parameter.RefKind != RefKind.None)
            {
                return null;
            }

            if (ContainsAnonymousType(parameter.Type) || TypeAccessibility.IsInaccessibleFromGenerated(parameter.Type))
            {
                return null;
            }

            var resolved = ClassifyParameter(parameter);
            if (resolved is null)
            {
                return null;
            }

            parameters.Add(resolved);
        }

        var (returnKind, returnTypeName, taskResultTypeName, asyncEnumElementTypeName) =
            ClassifyReturn(handler.ReturnType);

        return new(
            parameters.ToImmutableArray(),
            returnTypeName,
            returnKind,
            taskResultTypeName,
            asyncEnumElementTypeName);
    }

    private static bool ContainsAnonymousType(ITypeSymbol type)
    {
        if (type.IsAnonymousType)
        {
            return true;
        }

        if (type is INamedTypeSymbol named && named.IsGenericType)
        {
            foreach (var arg in named.TypeArguments)
            {
                if (ContainsAnonymousType(arg))
                {
                    return true;
                }
            }
        }

        if (type is IArrayTypeSymbol array)
        {
            return ContainsAnonymousType(array.ElementType);
        }

        return false;
    }

    private static HandlerParameter? ClassifyParameter(IParameterSymbol parameter)
    {
        var typeDisplay = parameter.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var typeNoQualifier = parameter.Type.ToDisplayString();

        var kind = ResolveKind(parameter.Type, typeNoQualifier);
        var (streamElement, streamShape) = ResolveStreamShape(parameter.Type);
        var defaultLiteral = parameter.HasExplicitDefaultValue
            ? FormatDefault(parameter.ExplicitDefaultValue, parameter.Type)
            : null;

        return new(
            parameter.Name,
            typeDisplay,
            kind,
            IsNullable(parameter.Type),
            parameter.HasExplicitDefaultValue,
            defaultLiteral,
            streamElement,
            streamShape);
    }

    private static bool IsNullable(ITypeSymbol type)
    {
        if (type.IsReferenceType)
        {
            // Annotated => declared as T?. NotAnnotated => declared as T in a nullable-enabled
            // context. None => no nullable context (legacy); treat as nullable for safety.
            return type.NullableAnnotation != NullableAnnotation.NotAnnotated;
        }

        return type.OriginalDefinition.SpecialType == SpecialType.System_Nullable_T;
    }

    private static SurefireParameterKind ResolveKind(ITypeSymbol type, string typeNoQualifier)
    {
        var fqn = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        if (fqn == "global::" + JobContextFullName || typeNoQualifier == JobContextFullName)
        {
            return SurefireParameterKind.JobContext;
        }

        if (fqn == "global::" + CancellationTokenFullName || typeNoQualifier == CancellationTokenFullName)
        {
            return SurefireParameterKind.CancellationToken;
        }

        if (fqn == "global::" + ServiceProviderFullName || typeNoQualifier == ServiceProviderFullName)
        {
            return SurefireParameterKind.ServiceProvider;
        }

        if (IsStreamShape(type))
        {
            return SurefireParameterKind.Stream;
        }

        // The generator can't statically prove DI vs. JSON without examining the runtime
        // ServiceCollection; defer to IServiceProviderIsService at registration time, matching
        // Minimal APIs' unattributed-parameter contract.
        return SurefireParameterKind.ServiceOrJson;
    }

    private static bool IsStreamShape(ITypeSymbol type) => ResolveStreamShape(type).Element is { };

    private static (string? Element, SurefireStreamShape? Shape) ResolveStreamShape(ITypeSymbol type)
    {
        if (type is IArrayTypeSymbol array)
        {
            return (array.ElementType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                SurefireStreamShape.Array);
        }

        if (type is INamedTypeSymbol named && named.IsGenericType)
        {
            var constructed = named.ConstructedFrom.ToDisplayString();
            var element = named.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            return constructed switch
            {
                "System.Collections.Generic.IAsyncEnumerable<T>" => (element, SurefireStreamShape.AsyncEnumerable),
                "System.Collections.Generic.List<T>"
                    or "System.Collections.Generic.IReadOnlyList<T>"
                    or "System.Collections.Generic.IList<T>"
                    or "System.Collections.Generic.IEnumerable<T>" => (element, SurefireStreamShape.List),
                _ => (null, null)
            };
        }

        return (null, null);
    }

    private static (SurefireReturnKind Kind, string ReturnTypeName, string? TaskResult, string? AsyncEnumElement)
        ClassifyReturn(ITypeSymbol returnType)
    {
        var returnTypeName = returnType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

        if (returnType.SpecialType == SpecialType.System_Void)
        {
            return (SurefireReturnKind.Void, returnTypeName, null, null);
        }

        if (returnType is INamedTypeSymbol named && named.IsGenericType)
        {
            var constructed = named.ConstructedFrom.ToDisplayString();
            var argument = named.TypeArguments[0];
            var argName = argument.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);

            if (constructed == "System.Threading.Tasks.Task<TResult>")
            {
                // Task<IAsyncEnumerable<U>> returns an async stream.
                if (argument is INamedTypeSymbol innerNamed
                    && innerNamed.IsGenericType
                    && innerNamed.ConstructedFrom.ToDisplayString() == "System.Collections.Generic.IAsyncEnumerable<T>")
                {
                    var elem = innerNamed.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                    return (SurefireReturnKind.AsyncEnumerable, returnTypeName, argName, elem);
                }

                return (SurefireReturnKind.TaskOfT, returnTypeName, argName, null);
            }

            if (constructed == "System.Threading.Tasks.ValueTask<TResult>")
            {
                if (argument is INamedTypeSymbol innerNamed
                    && innerNamed.IsGenericType
                    && innerNamed.ConstructedFrom.ToDisplayString() == "System.Collections.Generic.IAsyncEnumerable<T>")
                {
                    var elem = innerNamed.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
                    return (SurefireReturnKind.AsyncEnumerable, returnTypeName, argName, elem);
                }

                return (SurefireReturnKind.ValueTaskOfT, returnTypeName, argName, null);
            }

            if (constructed == "System.Collections.Generic.IAsyncEnumerable<T>")
            {
                return (SurefireReturnKind.AsyncEnumerable, returnTypeName, null, argName);
            }
        }

        if (returnType.ToDisplayString() == "System.Threading.Tasks.Task")
        {
            return (SurefireReturnKind.Task, returnTypeName, null, null);
        }

        if (returnType.ToDisplayString() == "System.Threading.Tasks.ValueTask")
        {
            return (SurefireReturnKind.ValueTask, returnTypeName, null, null);
        }

        return (SurefireReturnKind.Scalar, returnTypeName, null, null);
    }

    private static string FormatDefault(object? value, ITypeSymbol type)
    {
        if (value is null)
        {
            return "null";
        }

        return value switch
        {
            bool b => b ? "true" : "false",
            string s => "\"" + s.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"",
            char c => "'" + c + "'",
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? "default"
        };
    }
}
