namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// A SQL filter or action expression that Service Bus rejects when the rule is created. Syntax errors
/// carry Azure's message shape — <c>[Token line=…, column=…, Token in error=…, Additional details=…]</c>,
/// with a 1-based line and a 0-based column — and semantic errors carry Azure's text verbatim.
/// Derives from <see cref="ArgumentException"/> because that is what the SDK raises for both.
/// </summary>
public sealed class SqlSyntaxException : ArgumentException
{
    private SqlSyntaxException(string message, int line, int column, string tokenInError, string details,
        IReadOnlyList<string> expected)
        : base(message)
    {
        Line = line;
        Column = column;
        TokenInError = tokenInError;
        Details = details;
        Expected = expected;
    }

    /// <summary>1-based line of the offending token; 0 for semantic errors.</summary>
    public int Line { get; }

    /// <summary>0-based column of the offending token; 0 for semantic errors.</summary>
    public int Column { get; }

    public string TokenInError { get; }

    public string Details { get; }

    public IReadOnlyList<string> Expected { get; }

    /// <summary>
    /// A syntax error at <paramref name="position"/> in <paramref name="text"/>. A position at or past the end
    /// is reported the way Azure reports end of input: the line after the last one, column 0.
    /// </summary>
    public static SqlSyntaxException Syntax(string text, int position, string tokenInError, string details,
        IReadOnlyList<string>? expected = null)
    {
        var (line, column) = LineAndColumn(text, position);
        var message =
            $"There was an error parsing the SQL expression. [Token line={line}, column={column}, Token in error={tokenInError}, Additional details={details}]";
        return new SqlSyntaxException(message, line, column, tokenInError, details, expected ?? []);
    }

    /// <summary>A creation-time rejection whose message is Azure's text, verbatim.</summary>
    public static SqlSyntaxException Semantic(string message) => new(message, 0, 0, "", message, []);

    private static (int Line, int Column) LineAndColumn(string text, int position)
    {
        var line = 1;
        var lineStart = 0;
        var end = Math.Min(position, text.Length);
        for (var i = 0; i < end; i++)
        {
            if (text[i] != '\n') continue;
            line++;
            lineStart = i + 1;
        }

        return position >= text.Length ? (line + 1, 0) : (line, position - lineStart);
    }
}

/// <summary>
/// A filter or action that failed while evaluating a message. The broker dead-letters the message
/// (<c>FilterException</c> / <c>RuleActionException</c>) or drops it, as Azure does.
/// </summary>
public sealed class SqlEvaluationException(string message) : Exception(message);
