using Microsoft.CodeAnalysis;

namespace Surefire.SourceGeneration;

internal static class TypeAccessibility
{
    /// <summary>
    ///     Returns true when emitting a name-based reference to <paramref name="type" /> from the
    ///     <c>Surefire.Generated</c> namespace would fail to compile because the type (or some link
    ///     in its containment chain) is private/protected/file-scoped. Generic type arguments and
    ///     array element types are checked recursively.
    /// </summary>
    public static bool IsInaccessibleFromGenerated(ITypeSymbol type)
    {
        if (!IsReachable(type.DeclaredAccessibility))
        {
            return true;
        }

        for (var container = type.ContainingType; container is { }; container = container.ContainingType)
        {
            if (!IsReachable(container.DeclaredAccessibility))
            {
                return true;
            }
        }

        if (type is INamedTypeSymbol named && named.IsGenericType)
        {
            foreach (var arg in named.TypeArguments)
            {
                if (IsInaccessibleFromGenerated(arg))
                {
                    return true;
                }
            }
        }

        if (type is IArrayTypeSymbol array)
        {
            return IsInaccessibleFromGenerated(array.ElementType);
        }

        return false;
    }

    private static bool IsReachable(Accessibility accessibility) =>
        accessibility is Accessibility.Public or Accessibility.Internal or Accessibility.NotApplicable;
}
