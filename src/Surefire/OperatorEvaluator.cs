using System.Text.Json;
using System.Text.Json.Nodes;

namespace Surefire;

internal static class OperatorEvaluator
{
    /// <summary>
    /// Evaluates an operator step against a source JSON value and returns the result as serialized JSON.
    /// </summary>
    public static string Evaluate(string operatorType, JsonElement source, OperatorExpression? expression, JsonSerializerOptions jsonOptions)
    {
        return operatorType switch
        {
            "where" => EvaluateWhere(source, expression!, jsonOptions),
            "select" => EvaluateSelect(source, expression!, jsonOptions),
            "selectOne" => EvaluateSelectOne(source, expression!, jsonOptions),
            "count" => EvaluateCount(source),
            "sum" => EvaluateAggregate(source, expression, "sum", jsonOptions),
            "min" => EvaluateAggregate(source, expression, "min", jsonOptions),
            "max" => EvaluateAggregate(source, expression, "max", jsonOptions),
            "first" => EvaluateFirst(source, expression, required: true),
            "firstOrDefault" => EvaluateFirst(source, expression, required: false),
            "any" => EvaluateAny(source, expression),
            "all" => EvaluateAll(source, expression),
            "orderBy" => EvaluateOrderBy(source, expression!, ascending: true, jsonOptions),
            "orderByDescending" => EvaluateOrderBy(source, expression!, ascending: false, jsonOptions),
            _ => throw new NotSupportedException($"Operator type '{operatorType}' is not supported.")
        };
    }

    private static string EvaluateWhere(JsonElement source, OperatorExpression expression, JsonSerializerOptions jsonOptions)
    {
        var result = new JsonArray();
        foreach (var item in source.EnumerateArray())
        {
            if (EvaluateBool(expression, item))
                result.Add(JsonNode.Parse(item.GetRawText()));
        }
        return result.ToJsonString(jsonOptions);
    }

    private static string EvaluateSelect(JsonElement source, OperatorExpression expression, JsonSerializerOptions jsonOptions)
    {
        var result = new JsonArray();
        foreach (var item in source.EnumerateArray())
        {
            var projected = EvaluateExpression(expression, item);
            result.Add(ToJsonNode(projected));
        }
        return result.ToJsonString(jsonOptions);
    }

    private static string EvaluateSelectOne(JsonElement source, OperatorExpression expression, JsonSerializerOptions jsonOptions)
    {
        var projected = EvaluateExpression(expression, source);
        return JsonSerializer.Serialize(projected, jsonOptions);
    }

    private static string EvaluateCount(JsonElement source)
    {
        return source.GetArrayLength().ToString();
    }

    private static string EvaluateAggregate(JsonElement source, OperatorExpression? expression, string kind, JsonSerializerOptions jsonOptions)
    {
        var values = new List<double>();
        foreach (var item in source.EnumerateArray())
        {
            var val = expression is not null ? EvaluateExpression(expression, item) : item;
            values.Add(ConvertToDouble(val));
        }

        if (values.Count == 0)
            throw new InvalidOperationException($"Cannot compute {kind} of an empty sequence.");

        double result = kind switch
        {
            "sum" => values.Sum(),
            "min" => values.Min(),
            "max" => values.Max(),
            _ => throw new NotSupportedException()
        };

        return JsonSerializer.Serialize(result, jsonOptions);
    }

    private static string EvaluateFirst(JsonElement source, OperatorExpression? expression, bool required)
    {
        foreach (var item in source.EnumerateArray())
        {
            if (expression is null || EvaluateBool(expression, item))
                return item.GetRawText();
        }

        if (required)
            throw new InvalidOperationException("Sequence contains no matching element.");

        return "null";
    }

    private static string EvaluateAny(JsonElement source, OperatorExpression? expression)
    {
        if (expression is null)
            return source.GetArrayLength() > 0 ? "true" : "false";

        foreach (var item in source.EnumerateArray())
        {
            if (EvaluateBool(expression, item))
                return "true";
        }
        return "false";
    }

    private static string EvaluateAll(JsonElement source, OperatorExpression? expression)
    {
        if (expression is null)
            return "true";

        foreach (var item in source.EnumerateArray())
        {
            if (!EvaluateBool(expression, item))
                return "false";
        }
        return "true";
    }

    private static string EvaluateOrderBy(JsonElement source, OperatorExpression expression, bool ascending, JsonSerializerOptions jsonOptions)
    {
        var items = new List<(JsonElement item, object? key)>();
        foreach (var item in source.EnumerateArray())
            items.Add((item, EvaluateExpression(expression, item)));

        var sorted = ascending
            ? items.OrderBy(x => x.key as IComparable)
            : items.OrderByDescending(x => x.key as IComparable);

        var result = new JsonArray();
        foreach (var (item, _) in sorted)
            result.Add(JsonNode.Parse(item.GetRawText()));

        return result.ToJsonString(jsonOptions);
    }

    private static bool EvaluateBool(OperatorExpression expression, JsonElement item)
    {
        var result = EvaluateExpression(expression, item);
        return IsTruthy(result);
    }

    private static object? EvaluateExpression(OperatorExpression expression, JsonElement item) => expression switch
    {
        ParameterOp => ConvertJsonElement(item),
        ConstantOp c => c.Value is JsonElement el ? ConvertJsonElement(el) : c.Value,
        MemberAccessOp member => EvaluateMemberAccess(member, item),
        BinaryOp binary => EvaluateBinary(binary, item),
        UnaryOp unary => EvaluateUnary(unary, item),
        MethodCallOp call => EvaluateMethodCall(call, item),
        _ => throw new NotSupportedException($"Expression type '{expression.GetType().Name}' is not supported.")
    };

    private static object? EvaluateMemberAccess(MemberAccessOp member, JsonElement item)
    {
        var obj = EvaluateExpression(member.Object, item);
        if (obj is JsonElement el)
        {
            // Array.Length → array length
            if (el.ValueKind == JsonValueKind.Array && member.MemberName is "Length" or "Count")
                return (long)el.GetArrayLength();
            if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(member.MemberName, out var prop))
                return ConvertJsonElement(prop);
            // Try camelCase
            var camelCase = char.ToLowerInvariant(member.MemberName[0]) + member.MemberName[1..];
            if (el.ValueKind == JsonValueKind.Object && el.TryGetProperty(camelCase, out prop))
                return ConvertJsonElement(prop);
            return null;
        }

        if (obj is null) return null;

        // Reflect on CLR object
        var type = obj.GetType();
        var propInfo = type.GetProperty(member.MemberName);
        return propInfo?.GetValue(obj);
    }

    private static object? EvaluateBinary(BinaryOp binary, JsonElement item)
    {
        // Null-coalescing: short-circuit — only evaluate right if left is null
        if (binary.Operator == "??")
        {
            var left = EvaluateExpression(binary.Left, item);
            return left is null || (left is JsonElement el && el.ValueKind == JsonValueKind.Null)
                ? EvaluateExpression(binary.Right, item)
                : left;
        }

        var l = EvaluateExpression(binary.Left, item);
        var r = EvaluateExpression(binary.Right, item);

        return binary.Operator switch
        {
            "==" => Equals(l, r),
            "!=" => !Equals(l, r),
            "&&" => IsTruthy(l) && IsTruthy(r),
            "||" => IsTruthy(l) || IsTruthy(r),
            ">" => CompareValues(l, r) > 0,
            ">=" => CompareValues(l, r) >= 0,
            "<" => CompareValues(l, r) < 0,
            "<=" => CompareValues(l, r) <= 0,
            "+" when l is string || r is string => (l?.ToString() ?? "") + (r?.ToString() ?? ""),
            "+" => ArithmeticOp(l, r, (a, b) => a + b),
            "-" => ArithmeticOp(l, r, (a, b) => a - b),
            "*" => ArithmeticOp(l, r, (a, b) => a * b),
            "/" => ArithmeticOp(l, r, (a, b) => a / b),
            "%" => ArithmeticOp(l, r, (a, b) => a % b),
            _ => throw new NotSupportedException($"Binary operator '{binary.Operator}' is not supported.")
        };
    }

    private static object? EvaluateUnary(UnaryOp unary, JsonElement item)
    {
        var operand = EvaluateExpression(unary.Operand, item);
        return unary.Operator switch
        {
            "!" => !IsTruthy(operand),
            "-" => -ConvertToDouble(operand),
            _ => throw new NotSupportedException($"Unary operator '{unary.Operator}' is not supported.")
        };
    }

    private static object? EvaluateMethodCall(MethodCallOp call, JsonElement item)
    {
        if (call.Object is not null)
        {
            var obj = EvaluateExpression(call.Object, item)?.ToString() ?? "";
            var arg0 = call.Arguments.Count > 0 ? EvaluateExpression(call.Arguments[0], item)?.ToString() ?? "" : "";

            return call.MethodName switch
            {
                "Contains" => obj.Contains(arg0, StringComparison.Ordinal),
                "StartsWith" => obj.StartsWith(arg0, StringComparison.Ordinal),
                "EndsWith" => obj.EndsWith(arg0, StringComparison.Ordinal),
                "ToUpper" => obj.ToUpperInvariant(),
                "ToLower" => obj.ToLowerInvariant(),
                "Trim" => obj.Trim(),
                _ => throw new NotSupportedException($"Method '{call.MethodName}' is not supported.")
            };
        }

        // Static methods
        if (call.MethodName == "String.IsNullOrEmpty")
        {
            var arg = EvaluateExpression(call.Arguments[0], item)?.ToString();
            return string.IsNullOrEmpty(arg);
        }
        if (call.MethodName == "String.IsNullOrWhiteSpace")
        {
            var arg = EvaluateExpression(call.Arguments[0], item)?.ToString();
            return string.IsNullOrWhiteSpace(arg);
        }

        throw new NotSupportedException($"Static method '{call.MethodName}' is not supported.");
    }

    private static object? ConvertJsonElement(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.String => el.GetString(),
        JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Null => null,
        JsonValueKind.Undefined => null,
        // For objects and arrays, keep as JsonElement for nested member access
        _ => el
    };

    private static bool IsTruthy(object? value) => value switch
    {
        null => false,
        bool b => b,
        int i => i != 0,
        long l => l != 0,
        double d => d != 0,
        string s => s.Length > 0,
        JsonElement el => el.ValueKind is not (JsonValueKind.Null or JsonValueKind.False or JsonValueKind.Undefined),
        _ => true
    };

    private static new bool Equals(object? left, object? right)
    {
        if (left is null && right is null) return true;
        if (left is null || right is null) return false;

        // Numeric comparison: normalize to double
        if (IsNumeric(left) && IsNumeric(right))
            return ConvertToDouble(left) == ConvertToDouble(right);

        return left.Equals(right);
    }

    private static int CompareValues(object? left, object? right)
    {
        if (left is null && right is null) return 0;
        if (left is null) return -1;
        if (right is null) return 1;

        if (IsNumeric(left) && IsNumeric(right))
            return ConvertToDouble(left).CompareTo(ConvertToDouble(right));

        if (left is string sl && right is string sr)
            return string.Compare(sl, sr, StringComparison.Ordinal);

        if (left is IComparable cl)
            return cl.CompareTo(right);

        throw new InvalidOperationException($"Cannot compare values of type '{left.GetType().Name}' and '{right.GetType().Name}'.");
    }

    private static bool IsNumeric(object? value) => value is int or long or double or float or decimal;

    private static double ConvertToDouble(object? value) => value switch
    {
        int i => i,
        long l => l,
        double d => d,
        float f => f,
        decimal m => (double)m,
        JsonElement el when el.ValueKind == JsonValueKind.Number => el.GetDouble(),
        _ => throw new InvalidOperationException($"Cannot convert '{value?.GetType().Name ?? "null"}' to a numeric value.")
    };

    private static object? ArithmeticOp(object? left, object? right, Func<double, double, double> op)
    {
        var result = op(ConvertToDouble(left), ConvertToDouble(right));
        if (double.IsInfinity(result) || double.IsNaN(result))
            throw new InvalidOperationException($"Arithmetic operation produced {result} (left: {left}, right: {right}).");
        // Preserve integer type when possible, but only if the result fits in long range
        if (left is int or long && right is int or long
            && result == Math.Truncate(result)
            && result >= long.MinValue && result <= long.MaxValue)
            return (long)result;
        return result;
    }

    private static JsonNode? ToJsonNode(object? value) => value switch
    {
        null => null,
        bool b => JsonValue.Create(b),
        int i => JsonValue.Create(i),
        long l => JsonValue.Create(l),
        double d => JsonValue.Create(d),
        float f => JsonValue.Create(f),
        string s => JsonValue.Create(s),
        JsonElement el => JsonNode.Parse(el.GetRawText()),
        _ => JsonNode.Parse(JsonSerializer.Serialize(value))
    };
}
