using System.Text;
using Microsoft.CodeAnalysis.CSharp;

namespace Surefire.SourceGeneration;

internal static class DescriptorEmitter
{
    public static void EmitDescriptorBody(
        StringBuilder sb,
        int index,
        HandlerSignature handler,
        string? sourceCode)
    {
        sb.AppendLine("        return new Surefire.JobRegistrationDescriptor");
        sb.AppendLine("        {");
        sb.AppendLine("            Name = name,");
        sb.AppendLine("            Handler = handler,");
        if (sourceCode is { })
        {
            sb.Append("            SourceCode = ")
                .Append(SyntaxFactory.Literal(sourceCode).ToString())
                .AppendLine(",");
        }

        ParameterEmissionHelper.EmitParameters(sb, handler, true);
        ParameterEmissionHelper.EmitInvoke(sb, handler);
        EmitReturnInfo(sb, handler);
        EmitParameterTypeInfoFactories(sb, handler);
        EmitResultTypeInfoFactory(sb, handler);
        EmitOutputStreamElementTypeInfoFactory(sb, handler);
        EmitStreamParameterTypeInfoFactories(sb, handler);
        EmitMaterializer(sb, handler);
        EmitStreamBinders(sb, handler);
        EmitInputJsonBinders(sb, handler);
        sb.AppendLine("        };");
    }

    private static void EmitReturnInfo(StringBuilder sb, HandlerSignature handler)
    {
        sb.Append("            ReturnKind = global::Surefire.ReturnKind.").Append(handler.ReturnKind).AppendLine(",");
        sb.Append("            ReturnType = typeof(").Append(handler.ReturnTypeName).AppendLine("),");
        if (handler.AsyncEnumerableElementTypeName is { })
        {
            sb.Append("            AsyncEnumerableElementType = typeof(").Append(handler.AsyncEnumerableElementTypeName)
                .AppendLine("),");
        }

        switch (handler.ReturnKind)
        {
            case SurefireReturnKind.TaskOfT:
                sb.Append("            ExtractTaskResult = static t => ((global::System.Threading.Tasks.Task<")
                    .Append(handler.TaskResultTypeName).AppendLine(">)t).Result,");
                break;
            case SurefireReturnKind.ValueTaskOfT:
                sb.Append("            AsTask = static vt => ((global::System.Threading.Tasks.ValueTask<")
                    .Append(handler.TaskResultTypeName).AppendLine(">)vt).AsTask(),");
                sb.Append("            ExtractTaskResult = static t => ((global::System.Threading.Tasks.Task<")
                    .Append(handler.TaskResultTypeName).AppendLine(">)t).Result,");
                break;
        }
    }

    private static void EmitParameterTypeInfoFactories(StringBuilder sb, HandlerSignature handler)
    {
        // Per-parameter JsonTypeInfo factory: non-null only for JSON-bound parameters. The runtime
        // calls these lazily so a missing TypeInfo for an unused parameter doesn't fail registration.
        sb.AppendLine("            ParameterJsonTypeInfoFactories = new global::Surefire.JsonTypeInfoFactory?[]");
        sb.AppendLine("            {");
        foreach (var p in handler.Parameters)
        {
            if (p.Kind == SurefireParameterKind.Json || p.Kind == SurefireParameterKind.ServiceOrJson)
            {
                sb.Append("                static opts => opts.GetTypeInfo(typeof(").Append(p.TypeName)
                    .AppendLine(")),");
            }
            else
            {
                sb.AppendLine("                null,");
            }
        }

        sb.AppendLine("            },");
    }

    private static void EmitResultTypeInfoFactory(StringBuilder sb, HandlerSignature handler)
    {
        if (handler.ReturnKind is SurefireReturnKind.Void or SurefireReturnKind.Task or SurefireReturnKind.ValueTask)
        {
            return;
        }

        var typeName = handler.ReturnKind switch
        {
            SurefireReturnKind.TaskOfT or SurefireReturnKind.ValueTaskOfT => handler.TaskResultTypeName!,
            SurefireReturnKind.AsyncEnumerable => handler.AsyncEnumerableElementTypeName!,
            _ => handler.ReturnTypeName
        };

        sb.Append("            ResultJsonTypeInfoFactory = static opts => opts.GetTypeInfo(typeof(")
            .Append(typeName).AppendLine(")),");
    }

    private static void EmitOutputStreamElementTypeInfoFactory(StringBuilder sb, HandlerSignature handler)
    {
        if (handler.ReturnKind != SurefireReturnKind.AsyncEnumerable
            || handler.AsyncEnumerableElementTypeName is not { } elementType)
        {
            return;
        }

        sb.Append("            OutputStreamElementJsonTypeInfoFactory = static opts => opts.GetTypeInfo(typeof(")
            .Append(elementType).AppendLine(")),");
    }

    private static void EmitStreamParameterTypeInfoFactories(StringBuilder sb, HandlerSignature handler)
    {
        if (!handler.Parameters.Any(p => p.Kind == SurefireParameterKind.Stream))
        {
            return;
        }

        sb.AppendLine("            StreamParameterJsonTypeInfoFactories = new global::Surefire.JsonTypeInfoFactory?[]");
        sb.AppendLine("            {");
        foreach (var p in handler.Parameters)
        {
            if (p.Kind == SurefireParameterKind.Stream && p.StreamElementTypeName is { } elementType)
            {
                sb.Append("                static opts => opts.GetTypeInfo(typeof(").Append(elementType)
                    .AppendLine(")),");
            }
            else
            {
                sb.AppendLine("                null,");
            }
        }

        sb.AppendLine("            },");
    }

    private static void EmitMaterializer(StringBuilder sb, HandlerSignature handler)
    {
        if (handler.ReturnKind != SurefireReturnKind.AsyncEnumerable
            || handler.AsyncEnumerableElementTypeName is not { } elementType)
        {
            return;
        }

        // The runtime pre-resolves JsonTypeInfo via OutputStreamElementJsonTypeInfoFactory and
        // hands it to the materializer, so the closure only needs the strongly-typed dispatch.
        sb.AppendLine($$"""
                                    Materializer = static (pipe, stream, typeInfo, run, ct) =>
                                        pipe.WriteOutputStreamAsync<{{elementType}}>((global::System.Collections.Generic.IAsyncEnumerable<{{elementType}}>)stream, (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{{elementType}}>)typeInfo, run, ct),
                        """);
    }

    private static void EmitStreamBinders(StringBuilder sb, HandlerSignature handler)
    {
        if (!handler.Parameters.Any(p => p.Kind == SurefireParameterKind.Stream))
        {
            return;
        }

        sb.AppendLine("            StreamBinders = new global::Surefire.InputStreamBinder?[]");
        sb.AppendLine("            {");
        foreach (var p in handler.Parameters)
        {
            if (p.Kind != SurefireParameterKind.Stream || p.StreamElementTypeName is not { } elementType)
            {
                sb.AppendLine("                null,");
                continue;
            }

            var shape = p.StreamShape ?? SurefireStreamShape.AsyncEnumerable;
            var body = shape switch
            {
                SurefireStreamShape.AsyncEnumerable =>
                    $"return global::System.Threading.Tasks.Task.FromResult<object?>(pipe.ReadInputStreamAsync<{elementType}>(run, argName, ti, ct));",
                SurefireStreamShape.List =>
                    $$"""
                      return CollectListAsync(pipe, run, argName, ti, ct);
                                          static async global::System.Threading.Tasks.Task<object?> CollectListAsync(global::Surefire.IJobRunPipe pipe, global::Surefire.JobRun run, string argName, global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{{elementType}}> ti, global::System.Threading.CancellationToken ct) { var list = new global::System.Collections.Generic.List<{{elementType}}>(); await foreach (var item in pipe.ReadInputStreamAsync<{{elementType}}>(run, argName, ti, ct)) list.Add(item); return list; }
                      """,
                SurefireStreamShape.Array =>
                    $$"""
                      return CollectArrayAsync(pipe, run, argName, ti, ct);
                                          static async global::System.Threading.Tasks.Task<object?> CollectArrayAsync(global::Surefire.IJobRunPipe pipe, global::Surefire.JobRun run, string argName, global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{{elementType}}> ti, global::System.Threading.CancellationToken ct) { var list = new global::System.Collections.Generic.List<{{elementType}}>(); await foreach (var item in pipe.ReadInputStreamAsync<{{elementType}}>(run, argName, ti, ct)) list.Add(item); return list.ToArray(); }
                      """,
                _ => throw new InvalidOperationException($"Unhandled stream shape: {shape}")
            };

            // The runtime pre-resolves JsonTypeInfo via StreamParameterJsonTypeInfoFactories and
            // hands it to the binder, so the closure only needs the strongly-typed dispatch.
            sb.AppendLine($$"""
                                            static (pipe, run, argName, typeInfo, ct) =>
                                            {
                                                var ti = (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{{elementType}}>)typeInfo;
                                                {{body}}
                                            },
                            """);
        }

        sb.AppendLine("            },");
    }

    private static void EmitInputJsonBinders(StringBuilder sb, HandlerSignature handler)
    {
        // Stream-kind params can also receive their value as a JSON array on the run's Arguments
        // (e.g. trigger called with `new { values = new[] {1,2,3} }` instead of a streamed input).
        // Emit a closed-generic JSON deserializer + shape adapter so this works AOT-clean.
        if (!handler.Parameters.Any(p => p.Kind == SurefireParameterKind.Stream))
        {
            return;
        }

        sb.AppendLine(
            "            InputJsonBinders = new global::System.Func<global::System.Text.Json.JsonElement, global::System.Text.Json.JsonSerializerOptions, object?>?[]");
        sb.AppendLine("            {");
        foreach (var p in handler.Parameters)
        {
            if (p.Kind != SurefireParameterKind.Stream || p.StreamElementTypeName is not { } elementType)
            {
                sb.AppendLine("                null,");
                continue;
            }

            var shape = p.StreamShape ?? SurefireStreamShape.AsyncEnumerable;
            var body = shape switch
            {
                SurefireStreamShape.AsyncEnumerable =>
                    // Deserialize the JSON array as List<T>, then wrap it as IAsyncEnumerable<T>.
                    $$"""
                      var ti = (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::System.Collections.Generic.List<{{elementType}}>>)opts.GetTypeInfo(typeof(global::System.Collections.Generic.List<{{elementType}}>));
                                          var list = global::System.Text.Json.JsonSerializer.Deserialize(value.GetRawText(), ti) ?? new global::System.Collections.Generic.List<{{elementType}}>();
                                          return Yield(list);
                                          static async global::System.Collections.Generic.IAsyncEnumerable<{{elementType}}> Yield(global::System.Collections.Generic.List<{{elementType}}> source) { foreach (var item in source) { yield return item; await global::System.Threading.Tasks.Task.Yield(); } }
                      """,
                SurefireStreamShape.List =>
                    $$"""
                      var ti = (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<global::System.Collections.Generic.List<{{elementType}}>>)opts.GetTypeInfo(typeof(global::System.Collections.Generic.List<{{elementType}}>));
                                          return global::System.Text.Json.JsonSerializer.Deserialize(value.GetRawText(), ti);
                      """,
                SurefireStreamShape.Array =>
                    $$"""
                      var ti = (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{{elementType}}[]>)opts.GetTypeInfo(typeof({{elementType}}[]));
                                          return global::System.Text.Json.JsonSerializer.Deserialize(value.GetRawText(), ti);
                      """,
                _ => throw new InvalidOperationException($"Unhandled stream shape: {shape}")
            };

            sb.AppendLine($$"""
                                            static (value, opts) =>
                                            {
                                                {{body}}
                                            },
                            """);
        }

        sb.AppendLine("            },");
    }
}
