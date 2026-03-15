using System.Linq.Expressions;

namespace Surefire;

public static class StepLinqExtensions
{
    // -- Array operators on Step<T[]> --

    public static Step<T[]> Where<T>(this Step<T[]> source, Expression<Func<T, bool>> predicate)
        => CreateOperatorStep<T[], T[]>(source, "where", predicate);

    public static Step<TOut[]> Select<T, TOut>(this Step<T[]> source, Expression<Func<T, TOut>> selector)
        => CreateOperatorStep<T[], TOut[]>(source, "select", selector);

    public static Step<int> Count<T>(this Step<T[]> source)
        => CreateOperatorStep<T[], int>(source, "count", null);

    public static Step<double> Sum<T>(this Step<T[]> source, Expression<Func<T, double>> selector)
        => CreateOperatorStep<T[], double>(source, "sum", selector);

    public static Step<T> Min<T>(this Step<T[]> source, Expression<Func<T, T>>? selector = null)
        => CreateOperatorStep<T[], T>(source, "min", selector);

    public static Step<T> Max<T>(this Step<T[]> source, Expression<Func<T, T>>? selector = null)
        => CreateOperatorStep<T[], T>(source, "max", selector);

    public static Step<T> First<T>(this Step<T[]> source, Expression<Func<T, bool>>? predicate = null)
        => CreateOperatorStep<T[], T>(source, "first", predicate);

    public static Step<T?> FirstOrDefault<T>(this Step<T[]> source, Expression<Func<T, bool>>? predicate = null)
        => CreateOperatorStep<T[], T?>(source, "firstOrDefault", predicate);

    public static Step<bool> Any<T>(this Step<T[]> source, Expression<Func<T, bool>>? predicate = null)
        => CreateOperatorStep<T[], bool>(source, "any", predicate);

    public static Step<bool> All<T>(this Step<T[]> source, Expression<Func<T, bool>> predicate)
        => CreateOperatorStep<T[], bool>(source, "all", predicate);

    public static Step<T[]> OrderBy<T, TKey>(this Step<T[]> source, Expression<Func<T, TKey>> keySelector)
        => CreateOperatorStep<T[], T[]>(source, "orderBy", keySelector);

    public static Step<T[]> OrderByDescending<T, TKey>(this Step<T[]> source, Expression<Func<T, TKey>> keySelector)
        => CreateOperatorStep<T[], T[]>(source, "orderByDescending", keySelector);

    // -- Scalar projection on Step<T> --

    public static Step<TOut> Select<T, TOut>(this Step<T> source, Expression<Func<T, TOut>> selector)
        => CreateOperatorStep<T, TOut>(source, "selectOne", selector);

    // -- Core --

    private static Step<TOut> CreateOperatorStep<TSource, TOut>(Step<TSource> source, string operatorType, LambdaExpression? expression)
    {
        var builder = source.Builder;
        var stepId = builder.NextStepId();

        var operatorExpr = expression is not null ? ExpressionConverter.Convert(expression) : null;

        var step = new OperatorStep
        {
            Id = stepId,
            OperatorType = operatorType,
            SourceStepId = source.InternalStep.Id,
            Expression = operatorExpr
        };
        step.DependsOn.Add(source.InternalStep.Id);

        builder.AddStep(step);
        return new Step<TOut>(builder, step);
    }
}
