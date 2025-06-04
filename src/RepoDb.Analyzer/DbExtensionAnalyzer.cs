using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;

namespace RepoDb.Analyzer;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public class RepoDbEntityAnalyzer : DiagnosticAnalyzer
{
    private static readonly DiagnosticDescriptor Rule = new DiagnosticDescriptor(
        "REPDB001", "Invalid entity passed to RepoDB method", "Method '{0}' expects a single entity, not a collection but got an '{1}'. You might want to use the 'All' version of this function.", "Usage",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(Rule);

    public override void Initialize(AnalysisContext context)
    {
        context.EnableConcurrentExecution();
        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);

        context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
    }

    private static void AnalyzeInvocation(SyntaxNodeAnalysisContext context)
    {
        if (context.Node is not InvocationExpressionSyntax invocation)
            return;

        var semanticModel = context.SemanticModel;
        var symbolInfo = semanticModel.GetSymbolInfo(invocation.Expression);

        IMethodSymbol? methodSymbol = symbolInfo.Symbol as IMethodSymbol;
        if (methodSymbol is null && symbolInfo.CandidateSymbols.Length == 1)
            methodSymbol = symbolInfo.CandidateSymbols[0] as IMethodSymbol;

        if (methodSymbol is null)
            return;

        var originalMethod = methodSymbol.ReducedFrom ?? methodSymbol;
        var isExtension = methodSymbol.ReducedFrom is not null;

        var ns = originalMethod.ContainingNamespace.ToDisplayString();
        var methodName = originalMethod.Name;

        if (ns != "RepoDb" || !IsTargetMethod(methodName))
            return;

        var parameters = originalMethod.Parameters;
        var arguments = invocation.ArgumentList.Arguments;

        int entityParamIndex = isExtension ? 1 : 0;

        if (parameters.Length <= entityParamIndex || arguments.Count <= 0)
            return;

        var entityParam = parameters[entityParamIndex];

        // Only analyze if it's a method-level type parameter (e.g., TEntity)
        if (entityParam.Type is not ITypeParameterSymbol typeParam ||
            !originalMethod.TypeParameters.Contains(typeParam))
            return;

        var entityArgExpr = arguments[0].Expression;
        var argType = semanticModel.GetTypeInfo(entityArgExpr).Type;

        if (argType is null)
            return;

        // Exclude non-entity types
        if (
            argType.ToDisplayString() == "System.Dynamic.ExpandoObject" ||
            argType.AllInterfaces.Any(i => i.ToDisplayString().StartsWith("System.Collections.IDictionary")))
        {
            return;
        }

        // Is the passed argument a collection type?
        bool isCollection =
            argType.SpecialType == SpecialType.System_String ||
            argType.TypeKind == TypeKind.Array ||
            argType.AllInterfaces.Any(i => i.OriginalDefinition.ToDisplayString() == "System.Collections.IEnumerable" ||
                                           i.OriginalDefinition.ToDisplayString().StartsWith("System.Collections.Generic.IEnumerable"));

        if (isCollection)
        {
            var diagnostic = Diagnostic.Create(Rule, arguments[0].GetLocation(), methodName, argType.ToDisplayString());
            context.ReportDiagnostic(diagnostic);
        }
    }


    private static bool IsTargetMethod(string name) =>
        name is "Insert" or "Update" or "Delete" or "Merge" or "InsertAsync" or "UpdateAsync" or "DeleteAsync" or "MergeAsync";
}
