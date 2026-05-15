using System.Collections.Immutable;
using System.Text;

namespace Surefire.SourceGeneration;

/// <summary>
///     Emits interceptors for intercepted <see cref="Surefire.IJobClient" /> calls and forwards
///     them to AOT-safe overloads.
/// </summary>
internal static class IJobClientEmitter
{
    public static void EmitInterceptors(StringBuilder sb, ImmutableArray<IJobClientCall> calls)
    {
        if (calls.IsDefaultOrEmpty)
        {
            return;
        }

        for (var i = 0; i < calls.Length; i++)
        {
            var call = calls[i];
            EmitOne(sb, call, i);
        }
    }

    private static void EmitOne(StringBuilder sb, IJobClientCall call, int index)
    {
        switch (call.Method)
        {
            case IJobClientMethod.TriggerAsync:
                AppendInterceptsAttribute(sb, call);
                EmitForwarder(sb, call, index, "global::System.Threading.Tasks.Task<global::Surefire.JobRun>",
                    "TriggerAsync", "");
                break;
            case IJobClientMethod.RunAsync when call.ResultTypeName is { }:
                AppendInterceptsAttribute(sb, call);
                EmitForwarder(sb, call, index, "global::System.Threading.Tasks.Task<TResult>", "RunAsync", "<TResult>");
                break;
            case IJobClientMethod.RunAsync:
                AppendInterceptsAttribute(sb, call);
                EmitForwarder(sb, call, index, "global::System.Threading.Tasks.Task", "RunAsync", "");
                break;
            case IJobClientMethod.StreamAsync:
                AppendInterceptsAttribute(sb, call);
                EmitForwarder(sb, call, index, "global::System.Collections.Generic.IAsyncEnumerable<TResult>",
                    "StreamAsync", "<TResult>");
                break;
            case IJobClientMethod.WaitEachAsync:
                AppendInterceptsAttribute(sb, call);
                EmitWaitEachAsync(sb, call, index);
                break;
            case IJobClientMethod.TriggerBatchAsync:
                AppendInterceptsAttribute(sb, call);
                EmitBatchForwarder(sb, call, index, "global::System.Threading.Tasks.Task<global::Surefire.JobBatch>",
                    "TriggerBatchAsync", "");
                break;
            case IJobClientMethod.RunBatchAsync when call.ResultTypeName is { }:
                AppendInterceptsAttribute(sb, call);
                EmitBatchForwarder(sb, call, index,
                    "global::System.Threading.Tasks.Task<global::System.Collections.Generic.IReadOnlyList<TResult>>",
                    "RunBatchAsync", "<TResult>");
                break;
            case IJobClientMethod.RunBatchAsync:
                AppendInterceptsAttribute(sb, call);
                EmitBatchForwarder(sb, call, index, "global::System.Threading.Tasks.Task", "RunBatchAsync", "");
                break;
            case IJobClientMethod.StreamBatchAsync:
                AppendInterceptsAttribute(sb, call);
                EmitBatchForwarder(sb, call, index, "global::System.Collections.Generic.IAsyncEnumerable<TResult>",
                    "StreamBatchAsync", "<TResult>");
                break;
            case IJobClientMethod.BatchItemCreate:
                AppendInterceptsAttribute(sb, call);
                EmitBatchItemCreate(sb, call, index);
                break;
            default:
                return;
        }

        sb.AppendLine();
    }

    private static void AppendInterceptsAttribute(StringBuilder sb, IJobClientCall call)
    {
        sb.Append("    ").AppendLine(call.InterceptsLocationAttribute);
    }

    private static void EmitForwarder(StringBuilder sb, IJobClientCall call, int index,
        string returnType, string method, string typeArgs)
    {
        sb.AppendLine(
            $"    internal static {returnType} {method}_Client_{index}{typeArgs}(this {call.ReceiverTypeName} client, string job, object? args, global::Surefire.RunOptions? options = null, global::System.Threading.CancellationToken cancellationToken = default)");
        sb.AppendLine("    {");
        EmitBuildRunArgumentsCall(sb, call, index);
        sb.AppendLine($"        return client.{method}{typeArgs}(job, runArgs, options, cancellationToken);");
        sb.AppendLine("    }");
        EmitBuildRunArgumentsHelper(sb, call, index);
    }

    private static void EmitBatchForwarder(StringBuilder sb, IJobClientCall call, int index,
        string returnType, string method, string typeArgs)
    {
        sb.AppendLine(
            $"    internal static {returnType} {method}_Client_{index}{typeArgs}(this {call.ReceiverTypeName} client, string job, global::System.Collections.Generic.IEnumerable<object?> args, global::Surefire.BatchRunOptions? options = null, global::System.Threading.CancellationToken cancellationToken = default)");
        sb.AppendLine("    {");
        sb.AppendLine(
            $"        return client.{method}{typeArgs}(job, MapBatchArgs_{index}(args), options, cancellationToken);");
        sb.AppendLine("    }");
        EmitMapBatchArgsIterator(sb, call, index);
        EmitBuildRunArgumentsHelper(sb, call, index);
    }

    private static void EmitMapBatchArgsIterator(StringBuilder sb, IJobClientCall call, int index)
    {
        sb.AppendLine(
            $"    private static global::System.Collections.Generic.IEnumerable<global::Surefire.RunArguments?> MapBatchArgs_{index}(global::System.Collections.Generic.IEnumerable<object?> args)");
        sb.AppendLine("    {");
        sb.AppendLine("        foreach (var item in args)");
        sb.AppendLine("        {");
        EmitYieldBatchElement(sb, call, index);
        sb.AppendLine("        }");
        sb.AppendLine("    }");
    }

    private static void EmitYieldBatchElement(StringBuilder sb, IJobClientCall call, int index)
    {
        // InspectBatchArgs only marks anonymous element shapes for batch enumerables, so the
        // mapped item always flows through BuildArgs_N. Null items round-trip to null via the
        // same helper.
        sb.AppendLine($"            yield return BuildArgs_{index}(item);");
    }

    private static void EmitBatchItemCreate(StringBuilder sb, IJobClientCall call, int index)
    {
        sb.AppendLine(
            $"    internal static {call.ReceiverTypeName} Create_{index}(string jobName, object? args, global::Surefire.BatchRunOptions? options = null)");
        sb.AppendLine("    {");
        EmitBuildRunArgumentsCall(sb, call, index);
        sb.AppendLine($"        return new {call.ReceiverTypeName}(jobName, runArgs, options);");
        sb.AppendLine("    }");
        EmitBuildRunArgumentsHelper(sb, call, index);
    }

    private static void EmitWaitEachAsync(StringBuilder sb, IJobClientCall call, int index)
    {
        if (call.ResultIsAsyncEnumerable && call.ResultElementTypeName is { } elementT)
        {
            var streamOfStreams =
                $"global::System.Collections.Generic.IAsyncEnumerable<global::System.Collections.Generic.IAsyncEnumerable<{elementT}>>";
            sb.AppendLine($$"""
                                internal static {{streamOfStreams}} WaitEachAsync_Client_{{index}}<TResult>(this {{call.ReceiverTypeName}} client, string batchId, global::System.Threading.CancellationToken cancellationToken = default)
                                {
                                    return WaitEachLiveInner_{{index}}(client, batchId, cancellationToken);
                                }

                                private static async {{streamOfStreams}} WaitEachLiveInner_{{index}}({{call.ReceiverTypeName}} client, string batchId, [global::System.Runtime.CompilerServices.EnumeratorCancellation] global::System.Threading.CancellationToken cancellationToken)
                                {
                                    await foreach (var child in client.WaitEachAsync(batchId, cancellationToken))
                                    {
                                        yield return client.WaitStreamAsync<{{elementT}}>(child.Id, cancellationToken);
                                    }
                                }
                            """);
        }
    }

    private static void EmitBuildRunArgumentsCall(StringBuilder sb, IJobClientCall call, int index)
    {
        switch (call.ArgsShape)
        {
            case ArgsExpressionShape.Null:
                sb.AppendLine("        global::Surefire.RunArguments? runArgs = null;");
                break;
            case ArgsExpressionShape.RunArguments:
                sb.AppendLine("        var runArgs = (global::Surefire.RunArguments?)args;");
                break;
            case ArgsExpressionShape.Anonymous:
            case ArgsExpressionShape.NamedType:
                sb.AppendLine($"        var runArgs = BuildArgs_{index}(args);");
                break;
            default:
                sb.AppendLine("        var runArgs = args switch");
                sb.AppendLine("        {");
                sb.AppendLine("            null => null,");
                sb.AppendLine("            global::Surefire.RunArguments ra => ra,");
                sb.AppendLine(
                    "            _ => throw new global::System.InvalidOperationException(\"Surefire source generator could not statically resolve this args expression. Pass a RunArguments instance directly.\")");
                sb.AppendLine("        };");
                break;
        }
    }

    private static void EmitBuildRunArgumentsHelper(StringBuilder sb, IJobClientCall call, int index)
    {
        if (call.ArgsShape == ArgsExpressionShape.NamedType)
        {
            EmitNamedTypeBuildArgs(sb, call.NamedArgsTypeName!, index);
            return;
        }

        if (call.ArgsShape != ArgsExpressionShape.Anonymous)
        {
            return;
        }

        var shape = BuildAnonShapeLiteral(call.AnonProperties);
        var jsonProps = call.AnonProperties.Where(p => !p.IsStream).ToList();
        var streamProps = call.AnonProperties.Where(p => p.IsStream).ToList();

        sb.AppendLine($"    private static global::Surefire.RunArguments? BuildArgs_{index}(object? args)");
        sb.AppendLine("    {");
        sb.AppendLine("        if (args is null) return null;");
        sb.AppendLine($"        var typed = AnonAs(args, {shape});");

        if (streamProps.Count > 0)
        {
            sb.AppendLine("        var streams = new global::Surefire.RunArgumentStream[]");
            sb.AppendLine("        {");
            foreach (var stream in streamProps)
            {
                sb.AppendLine("            new global::Surefire.RunArgumentStream");
                sb.AppendLine("            {");
                sb.AppendLine($"                ArgumentName = \"{stream.Name}\",");
                sb.AppendLine(
                    $"                SerializeItems = opts => SerializeStream_{index}_{stream.Name}(typed.{stream.Name}, opts)");
                sb.AppendLine("            },");
            }

            sb.AppendLine("        };");
        }

        if (jsonProps.Count == 0)
        {
            if (streamProps.Count > 0)
            {
                sb.AppendLine("        return new global::Surefire.RunArguments { Streams = streams };");
            }
            else
            {
                sb.AppendLine("        return global::Surefire.RunArguments.Empty;");
            }
        }
        else
        {
            sb.AppendLine("        return new global::Surefire.RunArguments");
            sb.AppendLine("        {");
            sb.AppendLine("            WriteJson = (opts, writer) =>");
            sb.AppendLine("            {");
            sb.AppendLine("                writer.WriteStartObject();");
            foreach (var prop in jsonProps)
            {
                sb.AppendLine($"                writer.WritePropertyName(\"{prop.Name}\");");
                sb.AppendLine(
                    $"                var ti_{prop.Name} = (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{prop.TypeName}>)opts.GetTypeInfo(typeof({prop.TypeName}));");
                sb.AppendLine(
                    $"                global::System.Text.Json.JsonSerializer.Serialize(writer, typed.{prop.Name}, ti_{prop.Name});");
            }

            sb.AppendLine("                writer.WriteEndObject();");
            sb.AppendLine("            },");
            if (streamProps.Count > 0)
            {
                sb.AppendLine("            Streams = streams,");
            }

            sb.AppendLine("        };");
        }

        sb.AppendLine("    }");

        foreach (var stream in streamProps)
        {
            var elementT = stream.StreamElementTypeName!;
            sb.AppendLine(
                $"    private static async global::System.Collections.Generic.IAsyncEnumerable<string> SerializeStream_{index}_{stream.Name}(global::System.Collections.Generic.IAsyncEnumerable<{elementT}> source, global::System.Text.Json.JsonSerializerOptions options)");
            sb.AppendLine("    {");
            sb.AppendLine(
                $"        var ti = (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{elementT}>)options.GetTypeInfo(typeof({elementT}));");
            sb.AppendLine("        await foreach (var item in source)");
            sb.AppendLine("        {");
            sb.AppendLine("            yield return global::System.Text.Json.JsonSerializer.Serialize(item, ti);");
            sb.AppendLine("        }");
            sb.AppendLine("    }");
        }
    }

    private static void EmitNamedTypeBuildArgs(StringBuilder sb, string typeName, int index)
    {
        // Whole-object serialization via the runtime's resolved JsonTypeInfo<T>. This honors
        // [JsonPropertyName], naming policies, and any JsonSerializerContext configuration the
        // user wired up; per-property emission would silently bypass those.
        sb.AppendLine($"    private static global::Surefire.RunArguments? BuildArgs_{index}(object? args)");
        sb.AppendLine("    {");
        sb.AppendLine("        if (args is null) return null;");
        sb.AppendLine($"        var typed = ({typeName})args!;");
        sb.AppendLine("        return new global::Surefire.RunArguments");
        sb.AppendLine("        {");
        sb.AppendLine("            WriteJson = (opts, writer) =>");
        sb.AppendLine("            {");
        sb.AppendLine(
            $"                var ti = (global::System.Text.Json.Serialization.Metadata.JsonTypeInfo<{typeName}>)opts.GetTypeInfo(typeof({typeName}));");
        sb.AppendLine("                global::System.Text.Json.JsonSerializer.Serialize(writer, typed, ti);");
        sb.AppendLine("            },");
        sb.AppendLine("        };");
        sb.AppendLine("    }");
    }

    private static string BuildAnonShapeLiteral(EquatableArray<AnonArgProperty> props)
    {
        if (props.Length == 0)
        {
            return "new { }";
        }

        var sb = new StringBuilder("new { ");
        for (var i = 0; i < props.Length; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }

            var p = props[i];
            sb.Append(p.Name).Append(" = default(").Append(p.TypeName).Append(")!");
        }

        sb.Append(" }");
        return sb.ToString();
    }

    /// <summary>
    ///     Emits a single shared helper that the per-call-site <c>BuildArgs_N</c> methods use to
    ///     cast the user's <c>object?</c> args back to an anonymous-type shape. C# unifies
    ///     anonymous types structurally across a compilation, so the cast is type-safe.
    /// </summary>
    public static void EmitAnonHelper(StringBuilder sb)
    {
        sb.AppendLine("    private static T AnonAs<T>(object? args, T _) => (T)args!;");
    }
}
