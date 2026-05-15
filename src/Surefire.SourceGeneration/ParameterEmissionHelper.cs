using System.Text;

namespace Surefire.SourceGeneration;

internal static class ParameterEmissionHelper
{
    public static void EmitParameters(StringBuilder sb, HandlerSignature handler, bool emitStreamFields)
    {
        sb.AppendLine("            Parameters = new global::Surefire.ParameterDescriptor[]");
        sb.AppendLine("            {");
        foreach (var p in handler.Parameters)
        {
            sb.Append("                new global::Surefire.ParameterDescriptor(\"");
            sb.Append(p.Name).Append("\", typeof(").Append(p.TypeName).Append("), global::Surefire.ParameterKind.");
            sb.Append(p.Kind).Append(", HasDefault: ").Append(p.HasDefault ? "true" : "false");
            sb.Append(", DefaultValue: ").Append(p.HasDefault ? p.DefaultValueLiteral ?? "null" : "null");
            sb.Append(", IsNullable: ").Append(p.IsNullable ? "true" : "false");
            if (emitStreamFields && p.StreamElementTypeName is { })
            {
                sb.Append(", StreamElementType: typeof(").Append(p.StreamElementTypeName).Append(")");
                sb.Append(", StreamShape: global::Surefire.StreamShape.")
                    .Append(p.StreamShape ?? SurefireStreamShape.AsyncEnumerable);
            }

            sb.AppendLine("),");
        }

        sb.AppendLine("            },");
    }

    public static void EmitInvoke(StringBuilder sb, HandlerSignature handler)
    {
        var delegateType = handler.RenderDelegateType();
        if (handler.ReturnKind == SurefireReturnKind.Void)
        {
            // Void handlers return null so the executor knows there is no result to capture.
            sb.Append("            Invoke = static (args, h) => { ((").Append(delegateType).Append(")h)(");
            AppendArgList(sb, handler);
            sb.AppendLine("); return null; },");
            return;
        }

        sb.Append("            Invoke = static (args, h) => ((").Append(delegateType).Append(")h)(");
        AppendArgList(sb, handler);
        sb.AppendLine("),");
    }

    public static void AppendArgList(StringBuilder sb, HandlerSignature handler)
    {
        for (var i = 0; i < handler.Parameters.Length; i++)
        {
            if (i > 0)
            {
                sb.Append(", ");
            }

            var p = handler.Parameters[i];
            sb.Append('(').Append(p.TypeName).Append(")args[").Append(i).Append("]!");
        }
    }
}
