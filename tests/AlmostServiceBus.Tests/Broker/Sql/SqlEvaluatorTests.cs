using System.Globalization;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Tests.Broker.Sql;

/// <summary>Filter and action evaluation, with expected outcomes from the real-Azure probe.</summary>
public class SqlEvaluatorTests
{
    private static BrokeredMessage Message(params (string Key, object? Value)[] props)
    {
        var message = new BrokeredMessage();
        foreach (var (key, value) in props) message.ApplicationProperties[key] = value!;
        return message;
    }

    private static bool Matches(string filter, BrokeredMessage message, Dictionary<string, object?>? parameters = null) =>
        SqlFilter.Parse(filter, parameters).Matches(message);

    private static BrokeredMessage Apply(string action, BrokeredMessage message, Dictionary<string, object?>? parameters = null)
    {
        SqlRuleAction.Parse(action, parameters).Apply(message, "r0");
        return message;
    }

    // ── Issue #192 ───────────────────────────────────────────────────────────

    [Theory]
    [InlineData("messageType = 'Created'")]
    [InlineData("messageType IN ('Created', 'Updated')")]
    [InlineData("\"message-type\" = 'Created'")]
    [InlineData("[message-type] = 'Created'")]
    [InlineData("\"message-type\" IN ('Created', 'Updated')")]
    public void Issue192_OnlyMatchingValueMatches(string filter)
    {
        BrokeredMessage With(string v) => Message(("messageType", v), ("message-type", v));
        Assert.True(Matches(filter, With("Created")));
        Assert.False(Matches(filter, With("Unrelated")));
    }

    // ── Missing versus NULL ──────────────────────────────────────────────────

    [Theory]
    [InlineData("missing = 'x'")]
    [InlineData("missing <> 'x'")]
    [InlineData("NOT (missing = 'x')")]
    [InlineData("missing IN ('x')")]
    [InlineData("missing NOT IN ('x')")]
    [InlineData("missing NOT IN ('x', NULL)")]
    [InlineData("missing NOT LIKE 'x'")]
    [InlineData("NOT (missing < 1)")]
    [InlineData("NOT (missing > 1 AND 1 = 1)")]
    [InlineData("missing = NULL")]
    [InlineData("missing <> NULL")]
    [InlineData("missing1 = missing2")]
    [InlineData("missing + 1 = 1")]
    [InlineData("missing > 'a'")]
    [InlineData("missing = 1 OR missing = 'a'")]
    [InlineData("color LIKE missing")]
    [InlineData("property('missing') = 'x'")]
    [InlineData("EXISTS(Label)")]
    public void MissingProperty_IsUnknown(string filter) => Assert.False(Matches(filter, Message(("color", "red"))));

    [Theory]
    [InlineData("missing IS NULL")]
    [InlineData("NOT EXISTS(missing)")]
    [InlineData("missing = 'x' OR 1 = 1")]
    [InlineData("missing < 1 OR 1 = 1")]
    [InlineData("NOT (missing = 'x' AND 1 = 0)")]
    public void MissingProperty_DefiniteResults(string filter) => Assert.True(Matches(filter, Message()));

    [Theory]
    [InlineData("p = NULL", true)]
    [InlineData("p <> 'x'", true)]
    [InlineData("p = 'x'", false)]
    [InlineData("p IN ('x')", false)]
    [InlineData("p NOT IN ('x')", true)]
    [InlineData("p NOT LIKE 'x'", true)]
    [InlineData("p IS NULL", true)]
    [InlineData("EXISTS(p)", true)]
    public void NullValuedProperty_UsesCSharpNull(string filter, bool expected) =>
        Assert.Equal(expected, Matches(filter, Message(("p", null))));

    [Theory]
    [InlineData("NULL = NULL", true)]
    [InlineData("NULL <> NULL", false)]
    [InlineData("'a' = NULL", false)]
    [InlineData("color = NULL", false)]
    [InlineData("color <> NULL", true)]
    [InlineData("NOT (color = NULL)", true)]
    [InlineData("color IN ('red', NULL)", true)]
    [InlineData("color IN ('x', NULL)", false)]
    [InlineData("color NOT IN ('x', NULL)", true)]
    [InlineData("color NOT IN (NULL)", true)]
    public void NullLiteral_UsesCSharpNull(string filter, bool expected) =>
        Assert.Equal(expected, Matches(filter, Message(("color", "red"))));

    // ── Laziness and errors ──────────────────────────────────────────────────

    [Theory]
    [InlineData("1 = 1 OR color > 1", true)]
    [InlineData("1 = 0 AND color > 1", false)]
    [InlineData("color IN ('red', 1)", true)]
    public void ShortCircuit_SkipsTheErroringOperand(string filter, bool expected) =>
        Assert.Equal(expected, Matches(filter, Message(("color", "red"))));

    [Theory]
    [InlineData("color > 1 OR 1 = 1", "Operator '>' cannot be applied to operands of type 'string' and 'long'")]
    [InlineData("color > 1 AND 1 = 0", "Operator '>' cannot be applied to operands of type 'string' and 'long'")]
    [InlineData("color IN (1, 'red')", "Operator '==' cannot be applied to operands of type 'string' and 'long'")]
    [InlineData("NOT (color > 1)", "Operator '>' cannot be applied to operands of type 'string' and 'long'")]
    [InlineData("1 / 0 = 1", "Attempted to divide by zero.")]
    [InlineData("9223372036854775807 + 1 > 0", "Arithmetic operation resulted in an overflow.")]
    public void EvaluationErrors_Throw(string filter, string message) =>
        Assert.Equal(message,
            Assert.Throws<SqlEvaluationException>(() => Matches(filter, Message(("color", "red")))).Message);

    // ── Names, functions, system properties ──────────────────────────────────

    [Theory]
    [InlineData("user.Color = 'red'")]
    [InlineData("COLOR = 'red'")]
    [InlineData("property('COLOR') = 'red'")]
    [InlineData("PROPERTY(which) = 'red'")]
    [InlineData("color + 'x' = 'redx'")]
    public void UserNames_AreCaseInsensitive(string filter) =>
        Assert.True(Matches(filter, Message(("color", "red"), ("which", "color"))));

    [Fact]
    public void UnprefixedName_IsAUserProperty()
    {
        var message = new BrokeredMessage { Subject = "L" };
        Assert.False(Matches("label = 'L'", message));
        Assert.True(Matches("sys.label = 'L'", message));
        message.ApplicationProperties["label"] = "L";
        Assert.True(Matches("label = 'L'", message));
    }

    [Theory]
    [InlineData("sys.To IS NULL", true)]
    [InlineData("sys.To = NULL", true)]
    [InlineData("sys.To <> 'x'", true)]
    [InlineData("EXISTS(sys.To)", true)]
    [InlineData("sys.ScheduledEnqueueTimeUtc IS NULL", false)]
    [InlineData("sys.DeliveryCount = 1", true)]
    [InlineData("sys.DeliveryCount + 0.5 = 1.5", true)]
    [InlineData("sys.ExpiresAtUtc > sys.EnqueuedTimeUtc", true)]
    [InlineData("newid() = newid()", false)]
    [InlineData("property('sys.Label') = 'L'", false)]
    public void SystemPropertiesAndFunctions(string filter, bool expected) =>
        Assert.Equal(expected, Matches(filter, new BrokeredMessage { Subject = "L" }));

    [Fact]
    public void Guid_ComparesWithItsString()
    {
        var g = Guid.NewGuid();
        Assert.True(Matches($"g = '{g}'", Message(("g", g))));
    }

    [Fact]
    public void Parameters_BindCaseInsensitively() =>
        Assert.True(Matches("color = @P AND n IN (@a, @b)", Message(("color", "red"), ("n", 5L)),
            new() { ["@p"] = "red", ["@a"] = 1, ["@b"] = 5 }));

    [Fact]
    public void Parse_RejectsUnboundParameter() =>
        Assert.Throws<SqlSyntaxException>(() => SqlFilter.Parse("color = @missing"));

    // ── Actions ──────────────────────────────────────────────────────────────

    [Fact]
    public void Action_AddsRuleName_ThenRunsStatements()
    {
        var m = Apply("SET user.b = user.n * 2", Message(("n", 3)));
        Assert.Equal(6L, m.ApplicationProperties["b"]);
        Assert.Equal("r0", m.ApplicationProperties["RuleName"]);
    }

    [Fact]
    public void Action_SetExistingProperty_KeepsItsType()
    {
        var m = Apply("SET user.n = 5; SET user.m = user.n", Message(("n", 1)));
        Assert.Equal(5, m.ApplicationProperties["n"]);
        Assert.Equal(5, m.ApplicationProperties["m"]);
    }

    [Fact]
    public void Action_SetExistingPropertyCaseInsensitively_KeepsOriginalKey()
    {
        var m = Apply("SET user.N = 1.5", Message(("n", 1)));
        Assert.Equal(2, m.ApplicationProperties["n"]);
        Assert.False(m.ApplicationProperties.ContainsKey("N"));
    }

    [Fact]
    public void Action_ConversionFailure_KeepsEarlierEffects()
    {
        var m = Message(("color", "red"));
        var ex = Assert.Throws<SqlEvaluationException>(() => Apply("SET user.x = 1; SET user.x = 'a'", m));
        Assert.Equal("Input string was not in a correct format.", ex.Message);
        Assert.Equal(1L, m.ApplicationProperties["x"]);
    }

    [Fact]
    public void Action_RemoveThenRead_StoresNull()
    {
        var m = Apply("REMOVE user.color; SET user.x = user.color", Message(("color", "red")));
        Assert.False(m.ApplicationProperties.ContainsKey("color"));
        Assert.True(m.ApplicationProperties.ContainsKey("x"));
        Assert.Null(m.ApplicationProperties["x"]);
    }

    [Fact]
    public void Action_SetNullOrMissing_StoresNull()
    {
        var m = Apply("SET user.color = NULL; SET user.y = user.missing", Message(("color", "red")));
        Assert.Null(m.ApplicationProperties["color"]);
        Assert.Null(m.ApplicationProperties["y"]);
    }

    [Fact]
    public void Action_RemoveMissingUserProperty_IsNoOp() =>
        Assert.Equal("red", Apply("REMOVE user.nope", Message(("color", "red"))).ApplicationProperties["color"]);

    [Fact]
    public void Action_SetSenderPresetSystemProperty_IsReadableButNotDelivered()
    {
        var m = new BrokeredMessage { To = "o" };
        Apply("SET sys.To = 'new'; SET user.t = sys.To", m);
        Assert.Equal("new", m.ApplicationProperties["t"]);
        Assert.Equal("o", m.To);
    }

    [Fact]
    public void Action_SetUnsetSenderPresetSystemProperty_IsDelivered() =>
        Assert.Equal("v", Apply("SET sys.To = 'v'", new BrokeredMessage()).To);

    [Fact]
    public void Action_SetLabel_OverwritesSendersValue() =>
        Assert.Equal("new", Apply("SET sys.Label = 'new'", new BrokeredMessage { Subject = "o" }).Subject);

    [Fact]
    public void Action_SetPresetSessionId_ChangesOnlyPartitionKey()
    {
        var m = Apply("SET sys.SessionId = 'new'", new BrokeredMessage { SessionId = "o", PartitionKey = "o" });
        Assert.Equal("o", m.SessionId);
        Assert.Equal("new", m.PartitionKey);
    }

    [Fact]
    public void Action_SetSystemPropertyToNull_DoesNothing() =>
        Assert.Equal("r", Apply("SET sys.ReplyTo = NULL", new BrokeredMessage { ReplyTo = "r" }).ReplyTo);

    [Fact]
    public void Action_SetSystemProperty_ConvertsValue()
    {
        Assert.Equal("5", Apply("SET sys.Label = 5", new BrokeredMessage()).Subject);
        Assert.Equal(TimeSpan.FromHours(2), Apply("SET sys.TimeToLive = '02:00:00'", new BrokeredMessage()).TimeToLive);
        Assert.Equal("Invalid cast from 'System.Int64' to 'System.TimeSpan'.",
            Assert.Throws<SqlEvaluationException>(() => Apply("SET sys.TimeToLive = 5", new BrokeredMessage())).Message);
    }

    [Fact]
    public void Action_SetRuleName_Overrides() =>
        Assert.Equal("mine", Apply("SET user.RuleName = 'mine'", Message()).ApplicationProperties["RuleName"]);

    [Fact]
    public void Action_RemoveSystemProperty_FailsAtRunTime() =>
        Assert.Equal("A system property cannot be removed. The system property specified was 'Label'.\r\nParameter name: name",
            Assert.Throws<SqlEvaluationException>(() => Apply("REMOVE sys.Label", new BrokeredMessage())).Message);

    [Fact]
    public void Action_Parameters() =>
        Assert.Equal("pv", Apply("SET user.x = @v", Message(), new() { ["@v"] = "pv" }).ApplicationProperties["x"]);

    [Fact]
    public void Action_NewGuidIntoExistingString_StoresText()
    {
        var m = Apply("SET user.color = newid()", Message(("color", "red")));
        Assert.True(Guid.TryParse(Assert.IsType<string>(m.ApplicationProperties["color"]), out _));
    }

    [Fact]
    public void Action_StringConcatenation_UsesInvariantCulture()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("de-DE");
            Assert.Equal("red1.5", Apply("SET user.x = color + 1.5", Message(("color", "red"))).ApplicationProperties["x"]);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Fact]
    public void Action_WritingTimeSpan_EncodesAsSdkDescribedValue()
    {
        var m = Apply("SET user.ttl = sys.TimeToLive", new BrokeredMessage { TimeToLive = TimeSpan.FromHours(1) });
        Assert.IsType<global::Amqp.Types.DescribedValue>(m.ApplicationProperties["ttl"]);
    }
}
