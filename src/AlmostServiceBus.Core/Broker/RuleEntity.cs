using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// The filter type applied by a <see cref="RuleEntity"/>.
/// </summary>
public enum FilterType
{
    TrueFilter,
    FalseFilter,
    SqlFilter,
    CorrelationFilter
}

/// <summary>
/// A subscription rule that determines whether a published message should be
/// delivered to the owning subscription.
/// </summary>
public sealed class RuleEntity
{
    public string Name { get; set; } = string.Empty;

    public FilterType FilterType { get; set; } = FilterType.TrueFilter;

    /// <summary>SQL filter expression, used when <see cref="FilterType"/> is <see cref="FilterType.SqlFilter"/>.</summary>
    public string? SqlExpression { get; set; }

    /// <summary>Parameters bound to <see cref="SqlExpression"/>; keys include the leading <c>@</c>.</summary>
    public Dictionary<string, object?>? FilterParameters { get; set; }

    /// <summary>Correlation ID filter value, used when <see cref="FilterType"/> is <see cref="FilterType.CorrelationFilter"/>.</summary>
    public string? CorrelationId { get; set; }

    // ── Additional correlation filter properties ─────────────────────────────

    public string? Subject { get; set; }

    public string? To { get; set; }

    public string? ReplyTo { get; set; }

    public string? SessionId { get; set; }

    public string? ContentType { get; set; }

    /// <summary>Custom properties to match against <see cref="BrokeredMessage.ApplicationProperties"/>.</summary>
    public Dictionary<string, object>? CorrelationFilterProperties { get; set; }

    /// <summary>Optional SQL action expression executed on the copy this rule delivers.</summary>
    public string? ActionExpression { get; set; }

    /// <summary>Parameters bound to <see cref="ActionExpression"/>; keys include the leading <c>@</c>.</summary>
    public Dictionary<string, object?>? ActionParameters { get; set; }

    public bool HasAction => ActionExpression is not null;

    /// <summary>
    /// Evaluates whether this rule matches the given message.
    /// </summary>
    /// <exception cref="SqlSyntaxException">The rule was never validated and its SQL is invalid.</exception>
    /// <exception cref="SqlEvaluationException">The SQL filter failed on this message.</exception>
    public bool Matches(BrokeredMessage message)
    {
        return FilterType switch
        {
            FilterType.TrueFilter => true,
            FilterType.FalseFilter => false,
            FilterType.CorrelationFilter => MatchesCorrelationFilter(message),
            FilterType.SqlFilter => GetCompiled().Filter!.Matches(message),
            _ => throw new InvalidOperationException($"Unknown filter type {FilterType}.")
        };
    }

    /// <summary>Applies this rule's action to <paramref name="copy"/>, tagging it with this rule's name.</summary>
    /// <exception cref="SqlEvaluationException">A statement failed; earlier statements' effects remain.</exception>
    public void ApplyAction(BrokeredMessage copy) => GetCompiled().Action!.Apply(copy, Name);

    /// <summary>
    /// Compiles the SQL filter and action, throwing <see cref="SqlSyntaxException"/> with Azure's message if either
    /// is invalid. Called when the rule is added to a subscription.
    /// </summary>
    public void Validate() => GetCompiled();

    // The compiled filter and action, with the inputs they were built from. Swapped as one immutable reference,
    // so a publish racing an update sees either the old rule or the new one, never a mix.
    private sealed record Compiled(
        FilterType FilterType,
        string? FilterText,
        Dictionary<string, object?>? FilterParameters,
        SqlFilter? Filter,
        string? ActionText,
        Dictionary<string, object?>? ActionParameters,
        SqlRuleAction? Action);

    private Compiled? _compiled;

    private Compiled GetCompiled()
    {
        var (filterType, filterText, filterParameters) = (FilterType, SqlExpression, FilterParameters);
        var (actionText, actionParameters) = (ActionExpression, ActionParameters);

        var cached = _compiled;
        if (cached is not null && cached.FilterType == filterType && cached.FilterText == filterText
            && ReferenceEquals(cached.FilterParameters, filterParameters) && cached.ActionText == actionText
            && ReferenceEquals(cached.ActionParameters, actionParameters))
            return cached;

        var filter = filterType == FilterType.SqlFilter ? SqlFilter.Parse(filterText ?? "", filterParameters) : null;
        var action = actionText is null ? null : SqlRuleAction.Parse(actionText, actionParameters);
        var compiled = new Compiled(filterType, filterText, filterParameters, filter, actionText, actionParameters, action);
        _compiled = compiled;
        return compiled;
    }

    private bool MatchesCorrelationFilter(BrokeredMessage message)
    {
        if (CorrelationId is not null && !string.Equals(CorrelationId, message.CorrelationId, StringComparison.Ordinal))
            return false;
        if (Subject is not null && !string.Equals(Subject, message.Subject, StringComparison.Ordinal))
            return false;
        if (To is not null && !string.Equals(To, message.To, StringComparison.Ordinal))
            return false;
        if (ReplyTo is not null && !string.Equals(ReplyTo, message.ReplyTo, StringComparison.Ordinal))
            return false;
        if (SessionId is not null && !string.Equals(SessionId, message.SessionId, StringComparison.Ordinal))
            return false;
        if (ContentType is not null && !string.Equals(ContentType, message.ContentType, StringComparison.Ordinal))
            return false;

        // Match custom properties
        if (CorrelationFilterProperties is not null)
        {
            foreach (var (key, value) in CorrelationFilterProperties)
            {
                if (!message.ApplicationProperties.TryGetValue(key, out var msgValue))
                    return false;
                if (!Equals(value, msgValue))
                    return false;
            }
        }

        return true;
    }
}
