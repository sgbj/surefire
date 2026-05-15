using System.Text;

namespace Surefire.SourceGeneration;

internal sealed record HandlerSignature(
    EquatableArray<HandlerParameter> Parameters,
    string ReturnTypeName,
    SurefireReturnKind ReturnKind,
    string? TaskResultTypeName,
    string? AsyncEnumerableElementTypeName)
{
    public string RenderDelegateType()
    {
        var sb = new StringBuilder();
        if (Parameters.Length == 0)
        {
            return ReturnKind == SurefireReturnKind.Void ? "System.Action" : $"System.Func<{ReturnTypeName}>";
        }

        if (ReturnKind == SurefireReturnKind.Void)
        {
            sb.Append("System.Action<");
            for (var i = 0; i < Parameters.Length; i++)
            {
                if (i > 0)
                {
                    sb.Append(", ");
                }

                sb.Append(Parameters[i].TypeName);
            }

            sb.Append('>');
        }
        else
        {
            sb.Append("System.Func<");
            for (var i = 0; i < Parameters.Length; i++)
            {
                sb.Append(Parameters[i].TypeName).Append(", ");
            }

            sb.Append(ReturnTypeName).Append('>');
        }

        return sb.ToString();
    }
}

internal sealed record HandlerParameter(
    string Name,
    string TypeName,
    SurefireParameterKind Kind,
    bool IsNullable,
    bool HasDefault,
    string? DefaultValueLiteral,
    string? StreamElementTypeName,
    SurefireStreamShape? StreamShape);

internal enum SurefireParameterKind
{
    JobContext,
    CancellationToken,
    ServiceProvider,
    ServiceOrJson,
    Json,
    Stream
}

internal enum SurefireStreamShape
{
    AsyncEnumerable,
    List,
    Array
}

internal enum SurefireReturnKind
{
    Void,
    Task,
    ValueTask,
    TaskOfT,
    ValueTaskOfT,
    AsyncEnumerable,
    Scalar
}
