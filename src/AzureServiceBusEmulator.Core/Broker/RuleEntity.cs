using System.Text.RegularExpressions;

namespace AzureServiceBusEmulator.Core.Broker;

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

    // Regex for simple SQL equality expressions: property = 'value' or property = number
    private static readonly Regex SimpleEqualityPattern = new(
        @"^\s*([a-zA-Z_][\w.]*)\s*=\s*'([^']*)'\s*$",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex NumericEqualityPattern = new(
        @"^\s*([a-zA-Z_][\w.]*)\s*=\s*(\d+(?:\.\d+)?)\s*$",
        RegexOptions.Compiled | RegexOptions.IgnoreCase);

    private static readonly Regex TautologyPattern = new(
        @"^\s*1\s*=\s*1\s*$",
        RegexOptions.Compiled);

    private static readonly Regex ContradictionPattern = new(
        @"^\s*1\s*=\s*0\s*$",
        RegexOptions.Compiled);

    private bool MatchesSqlFilter(BrokeredMessage message)
    {
        if (string.IsNullOrWhiteSpace(SqlExpression))
            return true;

        try
        {
            return EvaluateExpression(message, SqlExpression.Trim());
        }
        catch
        {
            // For unrecognized expressions, default to match (permissive)
            return true;
        }
    }

    private static bool EvaluateExpression(BrokeredMessage message, string expr)
    {
        expr = expr.Trim();

        // Handle tautologies / contradictions
        if (TautologyPattern.IsMatch(expr) || expr.Equals("true", StringComparison.OrdinalIgnoreCase))
            return true;
        if (ContradictionPattern.IsMatch(expr) || expr.Equals("false", StringComparison.OrdinalIgnoreCase))
            return false;

        // Handle top-level parentheses: (expr)
        if (expr.StartsWith('(') && FindMatchingParen(expr, 0) == expr.Length - 1)
            return EvaluateExpression(message, expr[1..^1]);

        // Handle compound OR (lowest precedence — split on top-level OR)
        var orIdx = FindTopLevelBinaryOp(expr, "OR");
        if (orIdx >= 0)
        {
            return EvaluateExpression(message, expr[..orIdx])
                || EvaluateExpression(message, expr[(orIdx + 2)..]);
        }

        // Handle compound AND
        var andIdx = FindTopLevelBinaryOp(expr, "AND");
        if (andIdx >= 0)
        {
            return EvaluateExpression(message, expr[..andIdx])
                && EvaluateExpression(message, expr[(andIdx + 3)..]);
        }

        // Handle NOT expr (prefix NOT)
        if (expr.StartsWith("NOT ", StringComparison.OrdinalIgnoreCase) ||
            expr.StartsWith("NOT(", StringComparison.OrdinalIgnoreCase))
        {
            var inner = expr[3..].TrimStart();

            // NOT EXISTS(property)
            var notExistsMatch = Regex.Match(inner, @"^EXISTS\s*\(\s*([\w.]+)\s*\)\s*$", RegexOptions.IgnoreCase);
            if (notExistsMatch.Success)
            {
                var property = notExistsMatch.Groups[1].Value;
                return !PropertyExists(message, property);
            }

            return !EvaluateExpression(message, inner);
        }

        // Handle EXISTS(property)
        var existsMatch = Regex.Match(expr, @"^EXISTS\s*\(\s*([\w.]+)\s*\)\s*$", RegexOptions.IgnoreCase);
        if (existsMatch.Success)
        {
            var property = existsMatch.Groups[1].Value;
            return PropertyExists(message, property);
        }

        // Handle property LIKE 'pattern'
        var likeMatch = Regex.Match(expr, @"^([\w.]+)\s+LIKE\s+'([^']*)'\s*$", RegexOptions.IgnoreCase);
        if (likeMatch.Success)
        {
            var property = likeMatch.Groups[1].Value;
            var pattern = likeMatch.Groups[2].Value;
            return MatchesLikePattern(message, property, pattern);
        }

        // Handle property NOT LIKE 'pattern'
        var notLikeMatch = Regex.Match(expr, @"^([\w.]+)\s+NOT\s+LIKE\s+'([^']*)'\s*$", RegexOptions.IgnoreCase);
        if (notLikeMatch.Success)
        {
            var property = notLikeMatch.Groups[1].Value;
            var pattern = notLikeMatch.Groups[2].Value;
            return !MatchesLikePattern(message, property, pattern);
        }

        // Handle property = 'string-value'
        var stringMatch = SimpleEqualityPattern.Match(expr);
        if (stringMatch.Success)
        {
            var property = stringMatch.Groups[1].Value;
            var value = stringMatch.Groups[2].Value;
            return MatchesPropertyValue(message, property, value);
        }

        // Handle property = numeric-value
        var numericMatch = NumericEqualityPattern.Match(expr);
        if (numericMatch.Success)
        {
            var property = numericMatch.Groups[1].Value;
            var value = numericMatch.Groups[2].Value;
            return MatchesNumericPropertyValue(message, property, value);
        }

        // Handle property != 'string-value' and property <> 'string-value'
        var notEqualPattern = Regex.Match(expr, @"^([\w.]+)\s*(?:!=|<>)\s*'([^']*)'\s*$", RegexOptions.IgnoreCase);
        if (notEqualPattern.Success)
        {
            var property = notEqualPattern.Groups[1].Value;
            var value = notEqualPattern.Groups[2].Value;
            return !MatchesPropertyValue(message, property, value);
        }

        // Handle property IS NULL
        var isNullPattern = Regex.Match(expr, @"^([\w.]+)\s+IS\s+NULL\s*$", RegexOptions.IgnoreCase);
        if (isNullPattern.Success)
        {
            var property = isNullPattern.Groups[1].Value;
            return !PropertyExists(message, property);
        }

        // Handle property IS NOT NULL
        var isNotNullPattern = Regex.Match(expr, @"^([\w.]+)\s+IS\s+NOT\s+NULL\s*$", RegexOptions.IgnoreCase);
        if (isNotNullPattern.Success)
        {
            var property = isNotNullPattern.Groups[1].Value;
            return PropertyExists(message, property);
        }

        // For unrecognized expressions, default to match (permissive)
        return true;
    }

    /// <summary>
    /// Finds the index of a top-level binary operator (OR/AND) in <paramref name="expr"/>,
    /// respecting parentheses nesting.  Returns -1 if not found.
    /// </summary>
    private static int FindTopLevelBinaryOp(string expr, string op)
    {
        int depth = 0;
        for (int i = 0; i < expr.Length; i++)
        {
            char c = expr[i];
            if (c == '(') depth++;
            else if (c == ')') depth--;

            if (depth != 0) continue;

            if (i + op.Length <= expr.Length &&
                expr[i..].StartsWith(op, StringComparison.OrdinalIgnoreCase))
            {
                // Make sure it's surrounded by non-word chars (not part of a property name)
                bool leftOk = i == 0 || !char.IsLetterOrDigit(expr[i - 1]);
                bool rightOk = i + op.Length >= expr.Length || !char.IsLetterOrDigit(expr[i + op.Length]);
                if (leftOk && rightOk)
                    return i;
            }
        }
        return -1;
    }

    /// <summary>Returns the index of the closing parenthesis matching the opening at <paramref name="openIdx"/>.</summary>
    private static int FindMatchingParen(string expr, int openIdx)
    {
        int depth = 0;
        for (int i = openIdx; i < expr.Length; i++)
        {
            if (expr[i] == '(') depth++;
            else if (expr[i] == ')') { depth--; if (depth == 0) return i; }
        }
        return -1;
    }

    private static bool PropertyExists(BrokeredMessage message, string property)
    {
        // Strip the "user." prefix that Azure Service Bus uses for user-defined properties
        var key = property.StartsWith("user.", StringComparison.OrdinalIgnoreCase)
            ? property[5..]
            : property;

        var systemKey = key.ToLowerInvariant();
        if (systemKey is "correlationid" or "subject" or "label" or "to" or "replyto"
            or "sessionid" or "contenttype" or "messageid")
            return true; // system properties always exist on a message

        return message.ApplicationProperties.ContainsKey(key);
    }

    private static bool MatchesLikePattern(BrokeredMessage message, string property, string pattern)
    {
        // Resolve the property value
        var key = property.StartsWith("user.", StringComparison.OrdinalIgnoreCase)
            ? property[5..]
            : property;

        string? value = null;
        var systemKey = key.ToLowerInvariant();
        value = systemKey switch
        {
            "correlationid" => message.CorrelationId,
            "subject" or "label" => message.Subject,
            "to" => message.To,
            "replyto" => message.ReplyTo,
            "sessionid" => message.SessionId,
            "contenttype" => message.ContentType,
            "messageid" => message.MessageId,
            _ => null
        };

        if (value is null && message.ApplicationProperties.TryGetValue(key, out var appVal))
            value = appVal?.ToString();

        if (value is null) return false;

        // Convert LIKE pattern to Regex: % → .*, _ → .
        var regexPattern = "^" + Regex.Escape(pattern)
            .Replace("%", ".*", StringComparison.Ordinal)
            .Replace("_", ".", StringComparison.Ordinal) + "$";
        return Regex.IsMatch(value, regexPattern, RegexOptions.IgnoreCase);
    }

    private static bool MatchesNumericPropertyValue(BrokeredMessage message, string property, string value)
    {
        var key = property.StartsWith("user.", StringComparison.OrdinalIgnoreCase)
            ? property[5..]
            : property;

        if (message.ApplicationProperties.TryGetValue(key, out var msgValue))
        {
            // Compare numeric values — convert both sides to decimal for type-agnostic comparison
            if (decimal.TryParse(value, out var expected) &&
                decimal.TryParse(msgValue?.ToString(), out var actual))
                return expected == actual;
            return string.Equals(msgValue?.ToString(), value, StringComparison.Ordinal);
        }
        return false;
    }

    /// <summary>
    /// Matches a property name against both system properties and application properties.
    /// </summary>
    private static bool MatchesPropertyValue(BrokeredMessage message, string property, string value)
    {
        // Strip the "user." prefix used in ASB AMQP for user-defined properties
        var key = property.StartsWith("user.", StringComparison.OrdinalIgnoreCase)
            ? property[5..]
            : property;

        // Check system properties first
        var systemValue = key.ToLowerInvariant() switch
        {
            "correlationid" => message.CorrelationId,
            "subject" or "label" => message.Subject,
            "to" => message.To,
            "replyto" => message.ReplyTo,
            "sessionid" => message.SessionId,
            "contenttype" => message.ContentType,
            "messageid" => message.MessageId,
            _ => null
        };

        if (systemValue is not null)
            return string.Equals(systemValue, value, StringComparison.Ordinal);

        // Check application properties
        if (message.ApplicationProperties.TryGetValue(key, out var appValue))
            return string.Equals(appValue?.ToString(), value, StringComparison.Ordinal);

        return false;
    }
}
