using System.Text.Json.Serialization;

namespace Surefire;

[JsonPolymorphic(TypeDiscriminatorPropertyName = "op")]
[JsonDerivedType(typeof(MemberAccessOp), "member")]
[JsonDerivedType(typeof(ConstantOp), "const")]
[JsonDerivedType(typeof(ParameterOp), "param")]
[JsonDerivedType(typeof(BinaryOp), "binary")]
[JsonDerivedType(typeof(UnaryOp), "unary")]
[JsonDerivedType(typeof(MethodCallOp), "call")]
public abstract class OperatorExpression { }

public sealed class MemberAccessOp : OperatorExpression
{
    public required OperatorExpression Object { get; set; }
    public required string MemberName { get; set; }
}

public sealed class ConstantOp : OperatorExpression
{
    public required object? Value { get; set; }
}

public sealed class ParameterOp : OperatorExpression
{
    public required string Name { get; set; }
}

public sealed class BinaryOp : OperatorExpression
{
    public required string Operator { get; set; }
    public required OperatorExpression Left { get; set; }
    public required OperatorExpression Right { get; set; }
}

public sealed class UnaryOp : OperatorExpression
{
    public required string Operator { get; set; }
    public required OperatorExpression Operand { get; set; }
}

public sealed class MethodCallOp : OperatorExpression
{
    public OperatorExpression? Object { get; set; }
    public required string MethodName { get; set; }
    public List<OperatorExpression> Arguments { get; set; } = [];
}
