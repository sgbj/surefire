using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

namespace Surefire.SourceGeneration;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class SurefireDiagnosticSuppressor : DiagnosticSuppressor
{
    private static readonly SuppressionDescriptor TrimSuppression = new(
        "SPR0001",
        "IL2026",
        "The target Surefire call has been intercepted by a statically generated variant.");

    private static readonly SuppressionDescriptor AotSuppression = new(
        "SPR0002",
        "IL3050",
        "The target Surefire call has been intercepted by a statically generated variant.");

    public override ImmutableArray<SuppressionDescriptor> SupportedSuppressions { get; } =
    [
        TrimSuppression,
        AotSuppression
    ];

    public override void ReportSuppressions(SuppressionAnalysisContext context)
    {
        foreach (var diagnostic in context.ReportedDiagnostics)
        {
            var descriptor = diagnostic.Id switch
            {
                "IL2026" => TrimSuppression,
                "IL3050" => AotSuppression,
                _ => null
            };

            if (descriptor is null || !IsGeneratedSurefireCall(context, diagnostic))
            {
                continue;
            }

            context.ReportSuppression(Suppression.Create(descriptor, diagnostic));
        }
    }

    private static bool IsGeneratedSurefireCall(SuppressionAnalysisContext context, Diagnostic diagnostic)
    {
        if (!IsSourceGenerationEnabled(context))
        {
            return false;
        }

        var tree = diagnostic.Location.SourceTree;
        if (tree is null)
        {
            return false;
        }

        var cancellationToken = context.CancellationToken;
        var root = tree.GetRoot(cancellationToken);
        var node = root.FindNode(diagnostic.Location.SourceSpan, getInnermostNodeForTie: true);
        var invocation = node.FirstAncestorOrSelf<InvocationExpressionSyntax>();
        if (invocation is null)
        {
            return false;
        }

        var semanticModel = context.GetSemanticModel(tree);
        var symbol = semanticModel.GetSymbolInfo(invocation, cancellationToken).Symbol as IMethodSymbol;
        return symbol is not null
               && (IsGeneratedAddJobCall(semanticModel, invocation, symbol, cancellationToken)
                   || IsGeneratedCallbackCall(semanticModel, invocation, symbol, cancellationToken)
                   || IsGeneratedClientOrBatchItemCall(semanticModel, invocation, symbol, cancellationToken));
    }

    private static bool IsSourceGenerationEnabled(SuppressionAnalysisContext context) =>
        !context.Options.AnalyzerConfigOptionsProvider.GlobalOptions.TryGetValue(
            "build_property.SurefireSourceGenerationEnabled", out var value)
        || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    private static bool IsGeneratedAddJobCall(
        SemanticModel semanticModel,
        InvocationExpressionSyntax invocation,
        IMethodSymbol symbol,
        CancellationToken cancellationToken)
    {
        if (symbol.Name != "AddJob"
            || symbol.ContainingType?.ToDisplayString() != "Microsoft.Extensions.Hosting.HostExtensions"
            || symbol.TypeArguments.Length != 1
            || invocation.ArgumentList.Arguments.Count != 2)
        {
            return false;
        }

        var handlerExpression = invocation.ArgumentList.Arguments[1].Expression;
        var handlerSymbol = GetHandlerMethodSymbol(semanticModel, handlerExpression, cancellationToken);
        if (handlerSymbol is null || HandlerSignatureInspector.Inspect(handlerSymbol) is null)
        {
            return false;
        }

        return true;
    }

    private static bool IsGeneratedCallbackCall(
        SemanticModel semanticModel,
        InvocationExpressionSyntax invocation,
        IMethodSymbol symbol,
        CancellationToken cancellationToken)
    {
        if (symbol.Name is not ("OnSuccess" or "OnRetry" or "OnDeadLetter"))
        {
            return false;
        }

        if (symbol.ContainingType?.ToDisplayString() is not ("Surefire.JobBuilder" or "Surefire.SurefireOptions"))
        {
            return false;
        }

        if (symbol.Parameters.Length != 1
            || symbol.Parameters[0].Type.ToDisplayString() != "System.Delegate")
        {
            return false;
        }

        if (invocation.ArgumentList.Arguments.Count != 1)
        {
            return false;
        }

        var handlerExpression = invocation.ArgumentList.Arguments[0].Expression;
        var handlerSymbol = GetHandlerMethodSymbol(semanticModel, handlerExpression, cancellationToken);
        if (handlerSymbol is null || HandlerSignatureInspector.Inspect(handlerSymbol) is null)
        {
            return false;
        }

        return true;
    }

    private static bool IsGeneratedClientOrBatchItemCall(
        SemanticModel semanticModel,
        InvocationExpressionSyntax invocation,
        IMethodSymbol symbol,
        CancellationToken cancellationToken)
        => IJobClientCallInspector.Inspect(invocation, semanticModel, symbol, cancellationToken,
            requireInterceptableLocation: false) is not null;

    private static IMethodSymbol? GetHandlerMethodSymbol(SemanticModel model, ExpressionSyntax expression,
        CancellationToken cancellationToken)
    {
        var info = model.GetSymbolInfo(expression, cancellationToken);
        if (info.Symbol is IMethodSymbol method)
        {
            return method;
        }

        if (expression is AnonymousFunctionExpressionSyntax)
        {
            var op = model.GetOperation(expression, cancellationToken);
            if (op is IAnonymousFunctionOperation anon)
            {
                return anon.Symbol;
            }
        }

        return null;
    }
}
