using System.Text;

namespace Surefire.SourceGeneration;

/// <summary>
///     Emits the body of a generated <c>BuildCallbackDescriptor_N(Delegate)</c> factory.
///     Callbacks have a narrower shape than job handlers (no streams, no async-enumerable return,
///     no JSON-bound arguments), so this is intentionally simpler than
///     <see cref="DescriptorEmitter" />.
/// </summary>
internal static class CallbackDescriptorEmitter
{
    public static void EmitDescriptorBody(StringBuilder sb, HandlerSignature handler)
    {
        sb.AppendLine("        return new Surefire.CallbackDescriptor");
        sb.AppendLine("        {");
        sb.AppendLine("            Handler = handler,");
        ParameterEmissionHelper.EmitParameters(sb, handler, false);
        ParameterEmissionHelper.EmitInvoke(sb, handler);
        EmitReturnInfo(sb, handler);
        sb.AppendLine("        };");
    }

    private static void EmitReturnInfo(StringBuilder sb, HandlerSignature handler)
    {
        // CallbackDescriptor only carries ReturnKind + optional AsTask; no ReturnType, no
        // ExtractTaskResult (callback return values aren't surfaced back to the caller).
        sb.Append("            ReturnKind = global::Surefire.ReturnKind.").Append(handler.ReturnKind).AppendLine(",");
        if (handler.ReturnKind == SurefireReturnKind.ValueTaskOfT && handler.TaskResultTypeName is { } tResult)
        {
            sb.Append("            AsTask = static vt => ((global::System.Threading.Tasks.ValueTask<")
                .Append(tResult).AppendLine(">)vt).AsTask(),");
        }
    }
}
