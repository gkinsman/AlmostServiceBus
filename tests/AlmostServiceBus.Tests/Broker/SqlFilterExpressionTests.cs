using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

public class SqlFilterExpressionTests
{
    private static BrokeredMessage Message(params (string Key, object Value)[] props)
    {
        var msg = new BrokeredMessage();
        foreach (var (key, value) in props)
            msg.ApplicationProperties[key] = value;
        return msg;
    }

    private static bool Matches(string expression, BrokeredMessage message) =>
        new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = expression }.Matches(message);

    // ── Issue #192 ───────────────────────────────────────────────────────────

    [Theory]
    [InlineData("messageType = 'Created'")]
    [InlineData("messageType IN ('Created', 'Updated')")]
    [InlineData("\"message-type\" = 'Created'")]
    [InlineData("[message-type] = 'Created'")]
    [InlineData("\"message-type\" IN ('Created', 'Updated')")]
    [InlineData("user.[message-type] IN ('Created', 'Updated')")]
    public void Issue192_MatchesOnlyTheMatchingValue(string expression)
    {
        BrokeredMessage With(string value) => Message(("messageType", value), ("message-type", value));

        Assert.True(Matches(expression, With("Created")));
        Assert.False(Matches(expression, With("Unrelated")));
    }

    // ── IN ───────────────────────────────────────────────────────────────────

    [Fact]
    public void NotIn_ExcludesListedValues()
    {
        Assert.False(Matches("color NOT IN ('red', 'blue')", Message(("color", "red"))));
        Assert.True(Matches("color NOT IN ('red', 'blue')", Message(("color", "green"))));
    }

    [Fact]
    public void In_WithNumbers()
    {
        Assert.True(Matches("priority IN (1, 2, 3)", Message(("priority", 2))));
        Assert.False(Matches("priority IN (1, 2, 3)", Message(("priority", 4))));
    }

    [Fact]
    public void In_And_NotIn_AreUnknownWhenPropertyMissing()
    {
        Assert.False(Matches("color IN ('red')", Message()));
        Assert.False(Matches("color NOT IN ('red')", Message()));
    }

    // ── Delimited identifiers ────────────────────────────────────────────────

    [Fact]
    public void DelimitedIdentifiers_UnescapeDoubledDelimiters()
    {
        Assert.True(Matches("\"a\"\"b\" = 'x'", Message(("a\"b", "x"))));
        Assert.True(Matches("[a]]b] = 'x'", Message(("a]b", "x"))));
    }

    [Fact]
    public void DelimitedIdentifier_CanUseReservedWord()
    {
        Assert.True(Matches("[in] = 'x'", Message(("in", "x"))));
        Assert.True(Matches("\"and\" = 'x' AND [or] = 'y'", Message(("and", "x"), ("or", "y"))));
    }

    [Fact]
    public void StringLiteral_UnescapesDoubledQuote()
    {
        Assert.True(Matches("name = 'O''Brien'", Message(("name", "O'Brien"))));
    }

    // ── Scopes ───────────────────────────────────────────────────────────────

    [Fact]
    public void SystemScope_ReadsBrokerProperties()
    {
        var msg = new BrokeredMessage { Subject = "orders", CorrelationId = "c1", SessionId = "s1" };

        Assert.True(Matches("sys.Label = 'orders'", msg));
        Assert.True(Matches("sys.label = 'orders' AND sys.CorrelationId = 'c1'", msg));
        Assert.True(Matches("sys.[SessionId] = 's1'", msg));
        Assert.False(Matches("sys.To = 'anything'", msg));
        Assert.True(Matches("sys.To IS NULL", msg));
    }

    [Fact]
    public void UnscopedName_IsAUserProperty()
    {
        var msg = new BrokeredMessage { Subject = "orders" };
        msg.ApplicationProperties["label"] = "custom";

        Assert.True(Matches("label = 'custom'", msg));
        Assert.True(Matches("user.label = 'custom'", msg));
        Assert.False(Matches("label = 'orders'", msg));
    }

    // ── Three-valued logic and comparisons ───────────────────────────────────

    [Fact]
    public void MissingProperty_IsUnknown_NotMatchedByNegation()
    {
        Assert.False(Matches("color = 'red'", Message()));
        Assert.False(Matches("color <> 'red'", Message()));
        Assert.False(Matches("NOT (color = 'red')", Message()));
        Assert.True(Matches("color = 'red' OR 1 = 1", Message()));
        Assert.True(Matches("color IS NULL", Message()));
        Assert.False(Matches("color IS NOT NULL", Message()));
    }

    [Theory]
    [InlineData("quantity > 5", true)]
    [InlineData("quantity >= 10", true)]
    [InlineData("quantity < 10", false)]
    [InlineData("quantity <= 10", true)]
    [InlineData("quantity != 10", false)]
    [InlineData("quantity * 2 = 20", true)]
    [InlineData("quantity + 1.5 > 11", true)]
    [InlineData("quantity % 3 = 1", true)]
    [InlineData("-quantity = -10", true)]
    [InlineData("quantity / 0 = 1", false)]
    [InlineData("quantity = 1e1", true)]
    public void NumericComparisonsAndArithmetic(string expression, bool expected)
    {
        Assert.Equal(expected, Matches(expression, Message(("quantity", 10L))));
    }

    [Fact]
    public void NumericString_ComparesAsNumber()
    {
        Assert.True(Matches("ClientId = 27", Message(("ClientId", "27"))));
    }

    [Fact]
    public void Like_SupportsEscape()
    {
        Assert.True(Matches("code LIKE '50!%' ESCAPE '!'", Message(("code", "50%"))));
        Assert.False(Matches("code LIKE '50!%' ESCAPE '!'", Message(("code", "500"))));
        Assert.True(Matches("code LIKE 'a_c'", Message(("code", "abc"))));
    }

    [Fact]
    public void Precedence_AndBindsTighterThanOr()
    {
        var msg = Message(("a", 1), ("b", 2), ("c", 3));

        Assert.True(Matches("a = 1 OR b = 0 AND c = 0", msg));
        Assert.False(Matches("(a = 1 OR b = 0) AND c = 0", msg));
        Assert.True(Matches("NOT a = 2 AND b = 2", msg));
    }

    [Fact]
    public void BooleanProperty_CanBeUsedDirectly()
    {
        Assert.True(Matches("urgent", Message(("urgent", true))));
        Assert.False(Matches("urgent", Message(("urgent", false))));
        Assert.True(Matches("urgent = TRUE", Message(("urgent", true))));
    }

    // ── Rejection ────────────────────────────────────────────────────────────

    [Theory]
    [InlineData("color = ")]
    [InlineData("color IN ('a'")]
    [InlineData("color IN ()")]
    [InlineData("color === 'a'")]
    [InlineData("color = 'unterminated")]
    [InlineData("\"unterminated = 'a'")]
    [InlineData("[] = 'a'")]
    [InlineData("color 'a'")]
    [InlineData("sys.NotARealProperty = 'a'")]
    [InlineData("newid() = 'a'")]
    [InlineData("color NOT 'a'")]
    [InlineData("color LIKE 'a' ESCAPE 'xy'")]
    public void InvalidExpression_IsRejected(string expression)
    {
        Assert.Throws<SqlFilterException>(() => SqlFilterExpression.Parse(expression));
        Assert.Throws<SqlFilterException>(() => Matches(expression, Message(("color", "a"))));
    }

    [Fact]
    public void AddOrUpdateRule_RejectsInvalidSqlFilter()
    {
        var sub = new SubscriptionEntity("sub", "topic");
        var rule = new RuleEntity { Name = "bad", FilterType = FilterType.SqlFilter, SqlExpression = "color IN ('a'" };

        Assert.Throws<SqlFilterException>(() => sub.AddOrUpdateRule(rule));
        Assert.Null(sub.GetRule("bad"));
    }
}
