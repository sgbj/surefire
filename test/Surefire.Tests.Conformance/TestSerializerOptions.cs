using System.Text.Json.Serialization.Metadata;

namespace Surefire.Tests.Testing;

public static class TestSerializerOptions
{
    public static void AttachReflectionResolver(SurefireOptions options)
    {
        if (options.SerializerOptions.TypeInfoResolver is null
            && options.SerializerOptions.TypeInfoResolverChain.Count == 0)
        {
            options.SerializerOptions.TypeInfoResolver = new DefaultJsonTypeInfoResolver();
        }
    }
}
