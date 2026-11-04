namespace AlmostServiceBus.Core.Broker.Sql;

/// <summary>
/// A SQL filter or action expression that Service Bus rejects when the rule is created. Syntax errors
/// carry Azure's message shape — <c>[Token line=…, column=…, Token in error= …, Additional details= …]</c>,
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
            $"There was an error parsing the SQL expression. [Token line={line}, column={column}, Token in error= {tokenInError}, Additional details= {details}]";
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

/// <summary>Azure's wording for SQL rule errors, so emulator messages read the same as the service's.</summary>
internal static class SqlErrorText
{
    public static string UnknownGettable(string name) =>
        $"The specified property {name} does not exist, or it is not a gettable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class.\r\nParameter name: qualifiedPropertyName";

    public static string UnknownSettable(string name) =>
        $"The specified property ({name}) does not exist or it is not a settable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class.\r\nParameter name: property";

    public static string CannotRemoveSystem(string name) =>
        $"A system property cannot be removed. The system property specified was '{name}'.\r\nParameter name: name";

    public static string UnboundParameter(string name) =>
        $"A parameter ({name}) used in the expression specified in SqlExpression is missing from the Parameters property.";

    public static string UnusedParameter(string name) =>
        $"The specified parameter ({name}) in Parameters property was not used in the expression specified in SqlExpression.";

    public static string UnknownFunction(string name) =>
        $"The specified function name ({name}) is unknown. Make sure the function name is correct.";

    public static string FunctionArity(string name, int expected, int actual) =>
        $"The '{name}' function expects {expected} argument(s), but {actual} argument(s) was/were specified.\r\nParameter name: arguments";

    public static string LikeLiteral(Type type) =>
        $"Expression of type '{type.FullName}' cannot be used for parameter of type 'System.Object' of method 'System.Nullable`1[System.Boolean] LikeMethod(System.Object, System.Object, System.Object)'";
}
