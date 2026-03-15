using System.Linq.Expressions;

namespace Surefire;

internal static class ExpressionConverter
{
    private static readonly HashSet<string> SupportedStringMethods = ["Contains", "StartsWith", "EndsWith", "ToUpper", "ToLower", "Trim"];
    private static readonly HashSet<string> SupportedStaticMethods = ["IsNullOrEmpty", "IsNullOrWhiteSpace"];

    public static OperatorExpression Convert(LambdaExpression lambda)
    {
        if (lambda.Parameters.Count != 1)
            throw new NotSupportedException("Only single-parameter lambda expressions are supported.");
        return ConvertExpression(lambda.Body);
    }

    private static OperatorExpression ConvertExpression(Expression expr) => expr switch
    {
        ParameterExpression param => new ParameterOp { Name = param.Name! },
        ConstantExpression constant => new ConstantOp { Value = constant.Value },
        MemberExpression member => ConvertMemberExpression(member),
        BinaryExpression binary => ConvertBinaryExpression(binary),
        UnaryExpression unary => ConvertUnaryExpression(unary),
        MethodCallExpression call => ConvertMethodCallExpression(call),
        ConditionalExpression cond => throw new NotSupportedException("Ternary conditionals are not supported in operator expressions. Use plan-level If/Switch instead."),
        _ => throw new NotSupportedException($"Expression type '{expr.NodeType}' is not supported.")
    };

    private static OperatorExpression ConvertMemberExpression(MemberExpression member)
    {
        // Closure capture: member access on a constant (e.g., captured variable)
        if (member.Expression is ConstantExpression)
        {
            var value = Expression.Lambda(member).Compile().DynamicInvoke();
            return new ConstantOp { Value = value };
        }

        if (member.Expression is null)
        {
            // Static member access — evaluate
            var value = Expression.Lambda(member).Compile().DynamicInvoke();
            return new ConstantOp { Value = value };
        }

        return new MemberAccessOp
        {
            Object = ConvertExpression(member.Expression),
            MemberName = member.Member.Name
        };
    }

    private static OperatorExpression ConvertBinaryExpression(BinaryExpression binary)
    {
        var op = binary.NodeType switch
        {
            ExpressionType.Equal => "==",
            ExpressionType.NotEqual => "!=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.AndAlso => "&&",
            ExpressionType.OrElse => "||",
            ExpressionType.Add or ExpressionType.AddChecked => "+",
            ExpressionType.Subtract or ExpressionType.SubtractChecked => "-",
            ExpressionType.Multiply or ExpressionType.MultiplyChecked => "*",
            ExpressionType.Divide => "/",
            ExpressionType.Modulo => "%",
            ExpressionType.Coalesce => "??",
            _ => throw new NotSupportedException($"Binary operator '{binary.NodeType}' is not supported.")
        };

        return new BinaryOp
        {
            Operator = op,
            Left = ConvertExpression(binary.Left),
            Right = ConvertExpression(binary.Right)
        };
    }

    private static OperatorExpression ConvertUnaryExpression(UnaryExpression unary)
    {
        // Convert (type cast) — skip through to the operand
        if (unary.NodeType == ExpressionType.Convert || unary.NodeType == ExpressionType.ConvertChecked)
            return ConvertExpression(unary.Operand);

        // ArrayLength (e.g., arr.Length) — convert to member access on "Length"
        if (unary.NodeType == ExpressionType.ArrayLength)
            return new MemberAccessOp { Object = ConvertExpression(unary.Operand), MemberName = "Length" };

        var op = unary.NodeType switch
        {
            ExpressionType.Not => "!",
            ExpressionType.Negate or ExpressionType.NegateChecked => "-",
            _ => throw new NotSupportedException($"Unary operator '{unary.NodeType}' is not supported.")
        };

        return new UnaryOp
        {
            Operator = op,
            Operand = ConvertExpression(unary.Operand)
        };
    }

    private static OperatorExpression ConvertMethodCallExpression(MethodCallExpression call)
    {
        var methodName = call.Method.Name;

        if (call.Object is not null && SupportedStringMethods.Contains(methodName))
        {
            return new MethodCallOp
            {
                Object = ConvertExpression(call.Object),
                MethodName = methodName,
                Arguments = call.Arguments.Select(ConvertExpression).ToList()
            };
        }

        if (call.Object is null && call.Method.DeclaringType == typeof(string) && SupportedStaticMethods.Contains(methodName))
        {
            return new MethodCallOp
            {
                Object = null,
                MethodName = $"String.{methodName}",
                Arguments = call.Arguments.Select(ConvertExpression).ToList()
            };
        }

        throw new NotSupportedException($"Method '{call.Method.DeclaringType?.Name}.{methodName}' is not supported in operator expressions.");
    }
}
