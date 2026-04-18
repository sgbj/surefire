namespace Surefire;

internal static class TypeHelpers
{
    private static readonly Type AsyncEnumerableGenericDefinition = typeof(IAsyncEnumerable<>);

    public static bool TryGetAsyncEnumerableElementType(Type type, out Type elementType)
    {
        if (type.IsGenericType && type.GetGenericTypeDefinition() == AsyncEnumerableGenericDefinition)
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        var asyncEnumerable = type.GetInterfaces().FirstOrDefault(i =>
            i.IsGenericType && i.GetGenericTypeDefinition() == AsyncEnumerableGenericDefinition);
        if (asyncEnumerable is { })
        {
            elementType = asyncEnumerable.GetGenericArguments()[0];
            return true;
        }

        elementType = null!;
        return false;
    }

    public static bool TryGetCollectionElementType(Type targetType, out Type elementType, out bool asArray)
    {
        if (targetType.IsArray)
        {
            elementType = targetType.GetElementType()!;
            asArray = true;
            return true;
        }

        if (targetType.IsGenericType)
        {
            var definition = targetType.GetGenericTypeDefinition();
            if (definition == typeof(List<>)
                || definition == typeof(IReadOnlyList<>)
                || definition == typeof(IList<>)
                || definition == typeof(IEnumerable<>))
            {
                elementType = targetType.GetGenericArguments()[0];
                asArray = false;
                return true;
            }
        }

        elementType = null!;
        asArray = false;
        return false;
    }
}