namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>A compiled SQL filter: parsed, bound to its parameters, and immutable.</summary>
public sealed class SqlFilter
{
    private readonly SqlExpr _expression;

    private SqlFilter(string expression, IReadOnlyDictionary<string, object?> parameters, SqlExpr ast)
    {
        Expression = expression;
        Parameters = parameters;
        _expression = ast;
    }

    public string Expression { get; }

    public IReadOnlyDictionary<string, object?> Parameters { get; }

    /// <exception cref="SqlSyntaxException">The expression or its parameters are rejected, as Azure rejects them.</exception>
    public static SqlFilter Parse(string expression, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        var bound = BindParameters(parameters);
        var ast = SqlGrammar.ParseFilter(expression);
        SqlBinder.BindFilter(ast, bound);
        return new SqlFilter(expression, bound, ast);
    }

    /// <summary>True only when the filter is definitely true for the message.</summary>
    /// <exception cref="SqlEvaluationException">Evaluation failed; Azure dead-letters such messages.</exception>
    public bool Matches(BrokeredMessage message) =>
        SqlEvaluator.Evaluate(_expression, new SqlEvaluationContext(message, Parameters)) is true;

    internal static IReadOnlyDictionary<string, object?> BindParameters(IReadOnlyDictionary<string, object?>? parameters)
    {
        var bound = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, value) in parameters ?? new Dictionary<string, object?>())
            bound[name] = SqlValues.FromMessage(value);
        return bound;
    }
}
