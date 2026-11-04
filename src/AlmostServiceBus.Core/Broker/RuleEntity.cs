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

    /// <summary>Optional SQL action expression executed when the rule matches.</summary>
    public string? ActionExpression { get; set; }

    /// <summary>
    /// Evaluates whether this rule matches the given message.
    /// </summary>
    /// <exception cref="SqlFilterException">The SQL filter expression is invalid or unsupported.</exception>
    public bool Matches(BrokeredMessage message)
    {
        return FilterType switch
        {
            FilterType.TrueFilter => true,
            FilterType.FalseFilter => false,
            FilterType.CorrelationFilter => MatchesCorrelationFilter(message),
            FilterType.SqlFilter => MatchesSqlFilter(message),
            _ => true
        };
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

    private SqlFilterExpression? _parsedSql;

    /// <summary>
    /// Throws <see cref="SqlFilterException"/> if this is a SQL filter whose expression is invalid
    /// or unsupported. Called when the rule is added to a subscription.
    /// </summary>
    public void Validate()
    {
        if (FilterType == FilterType.SqlFilter)
            GetSqlFilter();
    }

    private bool MatchesSqlFilter(BrokeredMessage message) =>
        GetSqlFilter()?.Matches(message) ?? true;

    private SqlFilterExpression? GetSqlFilter()
    {
        if (string.IsNullOrWhiteSpace(SqlExpression))
            return null;

        if (_parsedSql is { } cached && cached.Text == SqlExpression)
            return cached;

        return _parsedSql = SqlFilterExpression.Parse(SqlExpression);
    }
}
