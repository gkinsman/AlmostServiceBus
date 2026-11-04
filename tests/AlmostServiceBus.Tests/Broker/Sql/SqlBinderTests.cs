using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Tests.Broker.Sql;

/// <summary>Creation-time semantic checks, with the messages real Azure returned.</summary>
public class SqlBinderTests
{
    private static readonly Dictionary<string, object?> NoParameters = [];

    private static void BindFilter(string text, Dictionary<string, object?>? parameters = null) =>
        SqlBinder.BindFilter(SqlGrammar.ParseFilter(text), parameters ?? NoParameters);

    private static void BindAction(string text, Dictionary<string, object?>? parameters = null) =>
        SqlBinder.BindAction(SqlGrammar.ParseAction(text), parameters ?? NoParameters);

    private static string Rejection(Action bind) => Assert.Throws<SqlSyntaxException>(bind).Message;

    [Theory]
    [InlineData("sys.Subject IS NOT NULL", "Subject")]
    [InlineData("sys.NoSuchProperty IS NULL", "NoSuchProperty")]
    [InlineData("EXISTS(sys.Nope)", "Nope")]
    public void UnknownSystemProperty_IsRejected(string text, string name) =>
        Assert.Equal(
            $"The specified property {name} does not exist, or it is not a gettable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class.\r\nParameter name: qualifiedPropertyName",
            Rejection(() => BindFilter(text)));

    [Theory]
    [InlineData("sys.label IS NULL")]
    [InlineData("sys.LABEL = 'L'")]
    [InlineData("sys.MessageId IS NOT NULL AND sys.CorrelationId IS NOT NULL AND sys.To IS NOT NULL AND sys.ReplyTo IS NOT NULL")]
    [InlineData("sys.SessionId IS NULL AND sys.ReplyToSessionId IS NULL AND sys.ContentType IS NULL AND sys.PartitionKey IS NULL")]
    [InlineData("sys.ViaPartitionKey IS NULL AND sys.SequenceNumber > 0 AND sys.EnqueuedSequenceNumber > 0 AND sys.DeliveryCount = 1")]
    [InlineData("sys.EnqueuedTimeUtc IS NOT NULL AND sys.ScheduledEnqueueTimeUtc IS NOT NULL AND sys.ExpiresAtUtc IS NOT NULL")]
    [InlineData("sys.TimeToLive IS NOT NULL AND sys.Size > 0 AND sys.LockToken IS NOT NULL AND sys.LockedUntilUtc IS NOT NULL")]
    [InlineData("sys.DeadLetterSource IS NULL AND sys.State IS NOT NULL AND sys.ForcePersistence IS NOT NULL")]
    public void ReadableSystemProperties_Bind(string text) => BindFilter(text);

    [Theory]
    [InlineData("SET sys.SequenceNumber = 5", "SequenceNumber")]
    [InlineData("SET sys.DeliveryCount = 5", "DeliveryCount")]
    [InlineData("SET sys.EnqueuedTimeUtc = 5", "EnqueuedTimeUtc")]
    [InlineData("SET sys.Size = 5", "Size")]
    [InlineData("SET sys.NoSuchProperty = 'v'", "NoSuchProperty")]
    public void UnsettableSystemTarget_IsRejected(string text, string name) =>
        Assert.Equal(
            $"The specified property ({name}) does not exist or it is not a settable property of the Microsoft.ServiceBus.Messaging.BrokeredMessage class.\r\nParameter name: property",
            Rejection(() => BindAction(text)));

    [Theory]
    [InlineData("SET sys.Label = 'x'; SET sys.To = 'x'; SET sys.ReplyTo = 'x'; SET sys.CorrelationId = 'x'")]
    [InlineData("SET sys.ContentType = 'x'; SET sys.ReplyToSessionId = 'x'; SET sys.SessionId = 'x'; SET sys.PartitionKey = 'x'")]
    [InlineData("SET sys.TimeToLive = '02:00:00'; SET sys.ScheduledEnqueueTimeUtc = '2020-01-01'; SET sys.MessageId = 'x'")]
    [InlineData("REMOVE sys.NoSuch")]
    [InlineData("REMOVE sys.Label")]
    public void SettableTargetsAndSystemRemoves_Bind(string text) => BindAction(text);

    [Fact]
    public void ActionExpression_ReadingUnknownSystemProperty_IsRejected() =>
        Assert.StartsWith("The specified property Nope does not exist, or it is not a gettable property",
            Rejection(() => BindAction("SET user.x = sys.Nope")));

    [Theory]
    [InlineData("upper(color) = 'RED'", "The specified function name (upper) is unknown. Make sure the function name is correct.")]
    [InlineData("newid(1) = 'x'", "The 'newid' function expects 0 argument(s), but 1 argument(s) was/were specified.\r\nParameter name: arguments")]
    [InlineData("property() = 'x'", "The 'property' function expects 1 argument(s), but 0 argument(s) was/were specified.\r\nParameter name: arguments")]
    public void BadFunctionCalls_AreRejected(string text, string message) =>
        Assert.Equal(message, Rejection(() => BindFilter(text)));

    [Fact]
    public void FunctionNames_AreCaseInsensitive() => BindFilter("NEWID() = NEWID() OR PROPERTY('a') = 1");

    [Theory]
    [InlineData("color LIKE 1", "System.Int64")]
    [InlineData("color LIKE 1.5", "System.Double")]
    [InlineData("color LIKE TRUE", "System.Boolean")]
    [InlineData("color LIKE 'a' ESCAPE 1", "System.Int64")]
    public void NonStringLiteralLikePattern_IsRejected(string text, string type) =>
        Assert.Equal(
            $"Expression of type '{type}' cannot be used for parameter of type 'System.Object' of method 'System.Nullable`1[System.Boolean] LikeMethod(System.Object, System.Object, System.Object)'",
            Rejection(() => BindFilter(text)));

    [Theory]
    [InlineData("color LIKE pat ESCAPE esc")]
    [InlineData("color LIKE @p")]
    [InlineData("color LIKE 'r' + '%'")]
    public void NonLiteralLikePattern_Binds(string text) =>
        BindFilter(text, text.Contains("@p") ? new() { ["@p"] = "r%" } : null);

    [Fact]
    public void UnboundParameter_IsRejected() =>
        Assert.Equal(
            "A parameter (@missing) used in the expression specified in SqlExpression is missing from the Parameters property.",
            Rejection(() => BindFilter("color = @missing", new() { ["@p"] = "red" })));

    [Fact]
    public void ParameterKeyWithoutAt_DoesNotBind() =>
        Assert.Equal(
            "A parameter (@p) used in the expression specified in SqlExpression is missing from the Parameters property.",
            Rejection(() => BindFilter("color = @p", new() { ["p"] = "red" })));

    [Fact]
    public void UnusedParameter_IsRejected() =>
        Assert.Equal(
            "The specified parameter (@unused) in Parameters property was not used in the expression specified in SqlExpression.",
            Rejection(() => BindFilter("color = 'red'", new() { ["@unused"] = "x" })));

    [Fact]
    public void ParameterNames_AreCaseInsensitive() => BindFilter("color = @P", new() { ["@p"] = "red" });

    [Fact]
    public void ActionParameters_AreBoundToo()
    {
        BindAction("SET user.x = @v", new() { ["@v"] = "pv" });
        Assert.StartsWith("A parameter (@nope)", Rejection(() => BindAction("SET user.x = @nope", new() { ["@v"] = "pv" })));
    }
}

public class SqlSystemPropertiesTests
{
    [Fact]
    public void Get_MapsLabelToSubject_CaseInsensitively()
    {
        var message = new BrokeredMessage { Subject = "orders" };
        Assert.Equal("orders", SqlSystemProperties.Get(message, "label"));
    }

    [Fact]
    public void Get_ScheduledEnqueueTimeUtc_IsMinValueWhenUnset() =>
        Assert.Equal(DateTime.MinValue, SqlSystemProperties.Get(new BrokeredMessage(), "ScheduledEnqueueTimeUtc"));

    [Fact]
    public void Get_DeliveryCount_IsOneWhenFiltersRun() =>
        Assert.Equal(1, SqlSystemProperties.Get(new BrokeredMessage(), "DeliveryCount"));

    [Fact]
    public void Get_EnqueuedSequenceNumber_EqualsSequenceNumber() =>
        Assert.Equal(42L, SqlSystemProperties.Get(new BrokeredMessage { SequenceNumber = 42 }, "EnqueuedSequenceNumber"));

    [Fact]
    public void Get_ExpiresAtUtc_SaturatesWhenTtlIsMax() =>
        Assert.Equal(DateTime.MaxValue, SqlSystemProperties.Get(new BrokeredMessage { TimeToLive = TimeSpan.MaxValue }, "ExpiresAtUtc"));

    [Fact]
    public void Get_Size_IsBodyLengthAsLong() =>
        Assert.Equal(3L, SqlSystemProperties.Get(new BrokeredMessage { Body = [1, 2, 3] }, "Size"));

    [Fact]
    public void Get_State_IsTheEnum() =>
        Assert.Equal(MessageState.Active, SqlSystemProperties.Get(new BrokeredMessage(), "State"));

    [Fact]
    public void Set_SessionId_AlsoSetsPartitionKey()
    {
        var message = new BrokeredMessage();
        SqlSystemProperties.Set(message, "SessionId", "s");
        Assert.Equal("s", message.SessionId);
        Assert.Equal("s", message.PartitionKey);
    }

    [Fact]
    public void Set_Label_SetsSubject()
    {
        var message = new BrokeredMessage();
        SqlSystemProperties.Set(message, "Label", "new");
        Assert.Equal("new", message.Subject);
    }

    [Theory]
    [InlineData("To", true)]
    [InlineData("MessageId", true)]
    [InlineData("SessionId", true)]
    [InlineData("Label", false)]
    [InlineData("ContentType", false)]
    public void SenderPresetWins_ListsTheQuirkProperties(string name, bool expected) =>
        Assert.Equal(expected, SqlSystemProperties.SenderPresetWins.Contains(name));
}
