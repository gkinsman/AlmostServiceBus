namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// A compiled SQL rule action (<c>SET</c>/<c>REMOVE</c> statements), applied to the copy of a message a rule
/// delivers. Behaviour follows Azure: the copy gets a <c>RuleName</c> property, statements run in order and see
/// each other's effects, and <c>SET</c> on an existing property converts to that property's type.
/// </summary>
public sealed class SqlRuleAction
{
    private readonly IReadOnlyList<SqlStatement> _statements;

    private SqlRuleAction(string expression, IReadOnlyDictionary<string, object?> parameters, IReadOnlyList<SqlStatement> statements)
    {
        Expression = expression;
        Parameters = parameters;
        _statements = statements;
    }

    public string Expression { get; }

    public IReadOnlyDictionary<string, object?> Parameters { get; }

    /// <exception cref="SqlSyntaxException">The action or its parameters are rejected, as Azure rejects them.</exception>
    public static SqlRuleAction Parse(string expression, IReadOnlyDictionary<string, object?>? parameters = null)
    {
        var bound = SqlFilter.BindParameters(parameters);
        var statements = SqlGrammar.ParseAction(expression);
        SqlBinder.BindAction(statements, bound);
        return new SqlRuleAction(expression, bound, statements);
    }

    /// <summary>
    /// Applies the action to <paramref name="copy"/>. On failure the effects of earlier statements stay on the
    /// copy, which Azure dead-letters with <c>RuleActionException</c>.
    /// </summary>
    /// <exception cref="SqlEvaluationException">A statement failed.</exception>
    public void Apply(BrokeredMessage copy, string ruleName)
    {
        copy.ApplicationProperties["RuleName"] = ruleName;

        var senderSet = SqlSystemProperties.SenderPresetWins
            .Where(name => SqlSystemProperties.Get(copy, name) is not null)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);
        var ctx = new SqlEvaluationContext(copy, Parameters);

        foreach (var statement in _statements)
        {
            switch (statement)
            {
                case SqlSet { Target.Scope: SqlScope.User } set:
                    SetUser(ctx, set.Target.Name, SqlEvaluator.Evaluate(set.Value, ctx));
                    break;

                case SqlSet set:
                    SetSystem(ctx, set.Target.Name, SqlEvaluator.Evaluate(set.Value, ctx), senderSet);
                    break;

                case SqlRemove { Target.Scope: SqlScope.User } remove:
                    if (ctx.FindUserKey(remove.Target.Name) is { } key)
                        copy.ApplicationProperties.Remove(key);
                    break;

                case SqlRemove remove:
                    throw new SqlEvaluationException(SqlErrorText.CannotRemoveSystem(remove.Target.Name));
            }
        }
    }

    private static void SetUser(SqlEvaluationContext ctx, string name, object? value)
    {
        var properties = ctx.Message.ApplicationProperties;
        if (value == SqlValues.Missing)
            value = null;

        var key = ctx.FindUserKey(name);
        if (key is not null && value is not null && SqlValues.FromMessage(properties[key]) is { } existing)
            value = SqlValues.ConvertTo(existing.GetType(), value);

        properties[key ?? name] = SqlValues.ToMessage(value)!;
    }

    // SET sys.X = NULL changes nothing. When the sender set one of the SenderPresetWins properties, the new
    // value is visible to later statements but the delivered message keeps the sender's (SessionId still moves
    // the partition key).
    private static void SetSystem(SqlEvaluationContext ctx, string name, object? value, HashSet<string> senderSet)
    {
        if (value is null || value == SqlValues.Missing)
            return;

        var converted = SqlValues.ConvertTo(SqlSystemProperties.TypeOf(name), value);
        ctx.SystemOverrides[name] = converted;

        if (!senderSet.Contains(name))
            SqlSystemProperties.Set(ctx.Message, name, converted);
        else if (name.Equals("SessionId", StringComparison.OrdinalIgnoreCase))
            ctx.Message.PartitionKey = (string?)converted;
    }
}
