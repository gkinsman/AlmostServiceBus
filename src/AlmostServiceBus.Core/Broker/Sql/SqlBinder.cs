namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// Creation-time semantic checks Azure applies after parsing: system property names, settable action targets,
/// functions and their arity, literal <c>LIKE</c> patterns, and parameter binding (every <c>@name</c> bound,
/// every supplied parameter used). Throws <see cref="SqlSyntaxException"/> with Azure's message.
/// </summary>
internal static class SqlBinder
{
    private static readonly Dictionary<string, int> FunctionArity = new()
    {
        ["newid"] = 0,
        ["property"] = 1,
    };

    public static void BindFilter(SqlExpr expr, IReadOnlyDictionary<string, object?> parameters)
    {
        var referenced = new List<string>();
        Guarded(() => Walk(expr, referenced));
        BindParameters(referenced, parameters);
    }

    // A very deep expression is rejected rather than allowed to overflow the stack.
    private static void Guarded(Action walk)
    {
        try
        {
            walk();
        }
        catch (InsufficientExecutionStackException)
        {
            throw SqlSyntaxException.Semantic("The SQL expression is too deeply nested.");
        }
    }

    public static void BindAction(IReadOnlyList<SqlStatement> statements, IReadOnlyDictionary<string, object?> parameters)
    {
        var referenced = new List<string>();
        foreach (var statement in statements)
        {
            switch (statement)
            {
                case SqlSet { Target: var target, Value: var value }:
                    if (target.Scope == SqlScope.System && !SqlSystemProperties.IsSettable(target.Name))
                        throw SqlSyntaxException.Semantic(SqlErrorText.UnknownSettable(target.Name));
                    Guarded(() => Walk(value, referenced));
                    break;
                case SqlRemove:
                    // REMOVE sys.X is accepted here and fails when the action runs, as on Azure.
                    break;
            }
        }

        BindParameters(referenced, parameters);
    }

    private static void Walk(SqlExpr expr, List<string> parameters)
    {
        System.Runtime.CompilerServices.RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (expr)
        {
            case SqlLiteral:
                break;
            case SqlPropertyRef property:
                CheckReadable(property);
                break;
            case SqlParameterRef parameter:
                parameters.Add(parameter.Name);
                break;
            case SqlUnary unary:
                Walk(unary.Operand, parameters);
                break;
            case SqlBinary binary:
                Walk(binary.Left, parameters);
                Walk(binary.Right, parameters);
                break;
            case SqlLike like:
                Walk(like.Operand, parameters);
                CheckLikeArgument(like.Pattern);
                Walk(like.Pattern, parameters);
                if (like.Escape is not null)
                {
                    CheckLikeArgument(like.Escape);
                    Walk(like.Escape, parameters);
                }
                break;
            case SqlIn @in:
                Walk(@in.Operand, parameters);
                foreach (var item in @in.Items) Walk(item, parameters);
                break;
            case SqlIsNull isNull:
                CheckReadable(isNull.Property);
                break;
            case SqlExists exists:
                CheckReadable(exists.Property);
                break;
            case SqlFunctionCall call:
                CheckFunction(call.Name, call.Arguments.Count);
                foreach (var argument in call.Arguments) Walk(argument, parameters);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(expr), expr.GetType().Name);
        }
    }

    /// <summary>Rejects an unknown function or a call with the wrong number of arguments.</summary>
    public static void CheckFunction(string name, int argumentCount)
    {
        if (!FunctionArity.TryGetValue(name, out var arity))
            throw SqlSyntaxException.Semantic(SqlErrorText.UnknownFunction(name));
        if (argumentCount != arity)
            throw SqlSyntaxException.Semantic(SqlErrorText.FunctionArity(name, arity, argumentCount));
    }

    private static void CheckReadable(SqlPropertyRef property)
    {
        if (property.Scope == SqlScope.System && !SqlSystemProperties.IsReadable(property.Name))
            throw SqlSyntaxException.Semantic(SqlErrorText.UnknownGettable(property.Name));
    }

    // Azure compiles LIKE to a method taking objects; a non-string literal fails that compilation.
    private static void CheckLikeArgument(SqlExpr argument)
    {
        if (argument is SqlLiteral { Value: { } value and not string })
            throw SqlSyntaxException.Semantic(SqlErrorText.LikeLiteral(value.GetType()));
    }

    private static void BindParameters(List<string> referenced, IReadOnlyDictionary<string, object?> parameters)
    {
        var supplied = new HashSet<string>(parameters.Keys, StringComparer.OrdinalIgnoreCase);
        foreach (var name in referenced)
        {
            if (!supplied.Contains(name))
                throw SqlSyntaxException.Semantic(SqlErrorText.UnboundParameter(name));
        }

        var used = new HashSet<string>(referenced, StringComparer.OrdinalIgnoreCase);
        foreach (var name in parameters.Keys)
        {
            if (!used.Contains(name))
                throw SqlSyntaxException.Semantic(SqlErrorText.UnusedParameter(name));
        }
    }
}
