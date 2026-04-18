using System.Linq.Expressions;
using System.Reflection;

namespace Surefire;

internal static class DelegateCompiler
{
    public static Func<object?[], object?> CompileInvoke(Delegate handler, ParameterInfo[] parameters)
    {
        var argsParam = Expression.Parameter(typeof(object?[]), "args");
        var callArgs = parameters.Select((p, i) =>
            (Expression)Expression.Convert(
                Expression.ArrayIndex(argsParam, Expression.Constant(i)),
                p.ParameterType)).ToArray();
        var call = Expression.Invoke(Expression.Constant(handler), callArgs);
        Expression body = handler.Method.ReturnType == typeof(void)
            ? Expression.Block(typeof(object), call, Expression.Constant(null, typeof(object)))
            : Expression.Convert(call, typeof(object));
        return Expression.Lambda<Func<object?[], object?>>(body, argsParam).Compile();
    }

    public static Func<object, Task>? CompileAsTask(Type returnType)
    {
        if (!returnType.IsGenericType || returnType.GetGenericTypeDefinition() != typeof(ValueTask<>))
        {
            return null;
        }

        var resultType = returnType.GetGenericArguments()[0];
        var vtParam = Expression.Parameter(typeof(object), "vt");
        var unboxed = Expression.Convert(vtParam, typeof(ValueTask<>).MakeGenericType(resultType));
        var call = Expression.Call(unboxed, typeof(ValueTask<>).MakeGenericType(resultType).GetMethod("AsTask")!);
        var body = Expression.Convert(call, typeof(Task));
        return Expression.Lambda<Func<object, Task>>(body, vtParam).Compile();
    }
}