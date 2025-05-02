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

        // Try to get the invoked method symbol
        IMethodSymbol? methodSymbol = symbolInfo.Symbol as IMethodSymbol;

        // If ambiguous (e.g., overload resolution), try first candidate
        if (methodSymbol is null && symbolInfo.CandidateSymbols.Length == 1)
        {
            methodSymbol = symbolInfo.CandidateSymbols[0] as IMethodSymbol;
        }

        if (methodSymbol is null)
            return;

        // For extension methods, get the original static method
        var originalMethod = methodSymbol.ReducedFrom ?? methodSymbol;

        var ns = originalMethod.ContainingNamespace.ToDisplayString();
        var typeName = originalMethod.ContainingType.ToDisplayString();
        var methodName = originalMethod.Name;

        // Only analyze methods from RepoDb.Extensions
        if (ns != "RepoDb" || !IsTargetMethod(methodName))
            return;

        // Get argument list (excluding 'this' parameter, which is implicit in extension methods)
        var arguments = invocation.ArgumentList.Arguments;
        if (arguments.Count < 1)
            return;

        var entityArg = arguments[0];
        var argType = semanticModel.GetTypeInfo(entityArg.Expression).Type;
        if (argType is null)
            return;

        // Exclude valid types: string, IDictionary, ExpandoObject
        if (argType.SpecialType == SpecialType.System_String ||
            argType.AllInterfaces.Any(i => i.ToDisplayString().StartsWith("System.Collections.IDictionary")) ||
            argType.ToDisplayString() == "System.Dynamic.ExpandoObject")
        {
            return;
        }

        // Flag if it's IEnumerable<T> (but not string)
        if (argType.TypeKind == TypeKind.Array
            || argType.AllInterfaces.Any(i => i.ToDisplayString().StartsWith("System.Collections.IEnumerable"))
            || argType.ToString().IndexOfAny(['`', '[']) >= 0)
        {
            var diagnostic = Diagnostic.Create(Rule, entityArg.GetLocation(), methodName, argType);
            context.ReportDiagnostic(diagnostic);
        }
    }

    private static bool IsTargetMethod(string name) =>
        name is "Insert" or "Update" or "Delete" or "Merge" or "InsertAsync" or "UpdateAsync" or "DeleteAsync" or "MergeAsync";
}
