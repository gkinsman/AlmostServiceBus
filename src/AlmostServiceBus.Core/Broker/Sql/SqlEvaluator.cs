namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// The message and bindings an expression is evaluated against. User property names are case-insensitive;
/// <see cref="SystemOverrides"/> holds <c>sys.</c> values an action has set, which later statements read.
/// </summary>
internal sealed class SqlEvaluationContext(BrokeredMessage message, IReadOnlyDictionary<string, object?> parameters)
{
    public BrokeredMessage Message { get; } = message;

    public Dictionary<string, object?> SystemOverrides { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>The value of a user property, or <see cref="SqlValues.Missing"/> when the message doesn't have it.</summary>
    public object? GetUser(string name) =>
        FindUserKey(name) is { } key ? SqlValues.FromMessage(Message.ApplicationProperties[key]) : SqlValues.Missing;

    public bool HasUser(string name) => FindUserKey(name) is not null;

    /// <summary>The message's own key for <paramref name="name"/> (exact match first), or null.</summary>
    public string? FindUserKey(string name)
    {
        var properties = Message.ApplicationProperties;
        if (properties.ContainsKey(name))
            return name;
        foreach (var key in properties.Keys)
        {
            if (string.Equals(key, name, StringComparison.OrdinalIgnoreCase))
                return key;
        }

        return null;
    }

    public object? GetSystem(string name) =>
        SystemOverrides.TryGetValue(name, out var value) ? value : SqlSystemProperties.Get(Message, name);

    public object? GetParameter(string name) => parameters[name];
}

/// <summary>
/// Evaluates a bound AST left to right with short-circuiting. Predicates yield <c>true</c>, <c>false</c> or
/// <see cref="SqlValues.Missing"/> (unknown); scalars yield CLR values, <c>null</c> or Missing.
/// </summary>
internal static class SqlEvaluator
{
    public static object? Evaluate(SqlExpr expr, SqlEvaluationContext ctx)
    {
        // An expression can nest about a thousand deep; fail the evaluation rather than overflow the stack.
        System.Runtime.CompilerServices.RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (expr)
        {
            case SqlLiteral literal:
                return literal.Value;

            case SqlPropertyRef { Scope: SqlScope.User } property:
                return ctx.GetUser(property.Name);

            case SqlPropertyRef property:
                return ctx.GetSystem(property.Name);

            case SqlParameterRef parameter:
                return ctx.GetParameter(parameter.Name);

            case SqlUnary unary:
                return EvaluateUnaryChain(unary, ctx);

            case SqlBinary { Op: SqlBinaryOp.And } and:
            {
                var left = Evaluate(and.Left, ctx);
                if (left is false) return false;
                var right = Evaluate(and.Right, ctx);
                if (right is false) return false;
                return left is true && right is true ? true : SqlValues.Missing;
            }

            case SqlBinary { Op: SqlBinaryOp.Or } or:
            {
                var left = Evaluate(or.Left, ctx);
                if (left is true) return true;
                var right = Evaluate(or.Right, ctx);
                if (right is true) return true;
                return left is false && right is false ? false : SqlValues.Missing;
            }

            case SqlBinary { Op: SqlBinaryOp.Add or SqlBinaryOp.Subtract or SqlBinaryOp.Multiply or SqlBinaryOp.Divide or SqlBinaryOp.Modulo } arithmetic:
                return SqlValues.Arithmetic(arithmetic.Op, Evaluate(arithmetic.Left, ctx), Evaluate(arithmetic.Right, ctx));

            case SqlBinary comparison:
                return SqlValues.Compare(comparison.Op, Evaluate(comparison.Left, ctx), Evaluate(comparison.Right, ctx));

            case SqlLike like:
            {
                var value = Evaluate(like.Operand, ctx);
                var pattern = Evaluate(like.Pattern, ctx);
                var escape = like.Escape is null ? null : Evaluate(like.Escape, ctx);
                return SqlValues.Like(value, pattern, escape) is { } matched
                    ? matched != like.Negated
                    : SqlValues.Missing;
            }

            case SqlIn @in:
                return EvaluateIn(@in, ctx);

            case SqlIsNull isNull:
            {
                var value = Evaluate(isNull.Property, ctx);
                return (value is null || value == SqlValues.Missing) != isNull.Negated;
            }

            case SqlExists { Property.Scope: SqlScope.User } exists:
                return ctx.HasUser(exists.Property.Name);

            case SqlExists:
                return true;

            case SqlFunctionCall { Name: "newid" }:
                return Guid.NewGuid();

            case SqlFunctionCall { Name: "property" } property:
                return Evaluate(property.Arguments[0], ctx) is string name ? ctx.GetUser(name) : SqlValues.Missing;

            default:
                throw new ArgumentOutOfRangeException(nameof(expr), expr.GetType().Name);
        }
    }

    // A run of unary operators (e.g. a thousand '-' or NOTs) is walked iteratively, innermost operator first, so it
    // costs no stack depth.
    private static object? EvaluateUnaryChain(SqlUnary outermost, SqlEvaluationContext ctx)
    {
        var operators = new List<SqlUnaryOp>();
        SqlExpr operand = outermost;
        while (operand is SqlUnary unary)
        {
            operators.Add(unary.Op);
            operand = unary.Operand;
        }

        var value = Evaluate(operand, ctx);
        for (var i = operators.Count - 1; i >= 0; i--)
        {
            value = operators[i] switch
            {
                SqlUnaryOp.Not => value is bool b ? !b : SqlValues.Missing,
                SqlUnaryOp.Negate => SqlValues.Negate(value),
                _ => value
            };
        }

        return value;
    }

    // True at the first equal item; otherwise unknown if anything was unknown, else false.
    private static object EvaluateIn(SqlIn @in, SqlEvaluationContext ctx)
    {
        var value = Evaluate(@in.Operand, ctx);
        if (value == SqlValues.Missing)
            return SqlValues.Missing;

        var unknown = false;
        foreach (var item in @in.Items)
        {
            var equal = SqlValues.Equal(value, Evaluate(item, ctx));
            if (equal is true)
                return !@in.Negated;
            if (equal == SqlValues.Missing)
                unknown = true;
        }

        return unknown ? SqlValues.Missing : @in.Negated;
    }
}
