using System.Diagnostics;
using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Broker.Sql;
using AlmostServiceBus.Core.Management;
using global::Amqp.Types;

namespace AlmostServiceBus.Tests.Broker.Sql;

/// <summary>Regression tests for the final branch review of #192.</summary>
public class ReviewFindingTests
{
    private static T OnSmallStack<T>(Func<T> work)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = work(); }
            catch (Exception ex) { error = ex; }
        }, 1024 * 1024);
        thread.Start();
        thread.Join();
        if (error is not null) throw error;
        return result;
    }

    // C1: a chain of unary operators recursed on the caller's stack and crashed the process (1 MB threads, as the
    // reviewer reproduced it: thread-pool and Kestrel request threads).
    [Fact]
    public void LongUnaryChain_InFilter_ParsesWithoutStackOverflow() =>
        Assert.True(OnSmallStack(() => SqlFilter.Parse("a = " + new string('-', 1000) + "1")
            .Matches(new BrokeredMessage { ApplicationProperties = { ["a"] = 1L } })));

    [Fact]
    public void LongUnaryChain_InAction_ParsesWithoutStackOverflow() =>
        Assert.NotNull(OnSmallStack(() => SqlRuleAction.Parse("SET a = " + new string('+', 1000) + "1")));

    [Fact]
    public void LongBinaryChain_NearMaxLength_EvaluatesOnAnOrdinaryStack()
    {
        var sum = string.Join("+", Enumerable.Repeat("1", 500));
        var text = "x = " + sum;
        Assert.True(text.Length <= SqlGrammar.MaxExpressionLength);

        Assert.True(OnSmallStack(() => SqlFilter.Parse(text)
            .Matches(new BrokeredMessage { ApplicationProperties = { ["x"] = 500L } })));
    }

    // I1: '%' wildcards compiled to a backtracking regex took tens of seconds on a few KB of text.
    [Fact]
    public void LikeWithSeveralWildcards_OnLongValue_RunsInLinearTime()
    {
        var value = string.Concat(Enumerable.Repeat("ab", 2500));
        var watch = Stopwatch.StartNew();

        Assert.Equal(false, SqlValues.Like(value, "%a%b%c%", null));
        Assert.Equal(false, SqlValues.Like(value[..200], string.Concat(Enumerable.Repeat("%a", 12)) + "c", null));

        Assert.True(watch.Elapsed < TimeSpan.FromSeconds(2), $"LIKE took {watch.Elapsed}");
    }

    // I2: slashed topic names (MassTransit-style) are valid entity names elsewhere in the emulator.
    [Theory]
    [InlineData("orders/Subscriptions/s", "orders", "s")]
    [InlineData("shop/orders/Subscriptions/s", "shop/orders", "s")]
    [InlineData("a/b/c/subscriptions/sub-1", "a/b/c", "sub-1")]
    public void SubscriptionAddress_SupportsSlashedTopicNames(string address, string topic, string subscription) =>
        Assert.Equal((topic, subscription), ManagementLinkEndpoint.ParseSubscriptionAddress(address));

    [Theory]
    [InlineData("orders")]
    [InlineData("orders/Subscriptions/")]
    [InlineData("/Subscriptions/s")]
    public void SubscriptionAddress_RejectsNonSubscriptions(string address) =>
        Assert.Null(ManagementLinkEndpoint.ParseSubscriptionAddress(address));

    // M2: an enum value written by SET couldn't be AMQP-encoded when the copy was delivered.
    [Fact]
    public void SetFromEnumSystemProperty_StoresAnEncodableValue()
    {
        var message = new BrokeredMessage();
        SqlRuleAction.Parse("SET user.state = sys.State").Apply(message, "r");

        var stored = message.ApplicationProperties["state"];
        Assert.IsType<int>(stored);
        var amqp = new global::Amqp.Message("b") { ApplicationProperties = new global::Amqp.Framing.ApplicationProperties { ["state"] = stored } };
        Assert.NotNull(amqp.Encode());
    }

    // M4: a malformed described URI threw UriFormatException out of Publish, skipping later subscriptions.
    [Fact]
    public void MalformedDescribedUri_DoesNotThrowFromDecoding()
    {
        var raw = new DescribedValue(new Symbol("com.microsoft:uri"), "http://[bad");
        var value = SqlValues.FromMessage(raw);
        Assert.NotNull(value);
    }

    [Fact]
    public void MalformedDescribedUri_DoesNotAbortPublishToOtherSubscriptions()
    {
        var topic = new TopicEntity("t");
        var filtered = topic.AddSubscription("filtered");
        filtered.RemoveRule("$Default");
        filtered.AddOrUpdateRule(new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = "u = 'x'" });
        var plain = topic.AddSubscription("plain");

        topic.Publish(new BrokeredMessage { ApplicationProperties = { ["u"] = new DescribedValue(new Symbol("com.microsoft:uri"), "http://[bad") } });

        Assert.NotNull(plain.Queue.TryDequeueImmediate());
    }

    // M7: an admin-client correlation filter on MessageId / ReplyToSessionId was read as an empty (match-all) filter.
    [Fact]
    public void AtomXmlCorrelationFilter_ReadsMessageIdAndReplyToSessionId()
    {
        const string xml = """
            <entry xmlns="http://www.w3.org/2005/Atom">
              <content type="application/xml">
                <RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"
                                 xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <Filter i:type="CorrelationFilter">
                    <MessageId>m1</MessageId>
                    <ReplyToSessionId>rs1</ReplyToSessionId>
                  </Filter>
                  <Name>corr</Name>
                </RuleDescription>
              </content>
            </entry>
            """;

        var props = AtomXmlReader.ReadRuleProperties(xml);

        Assert.Equal("m1", props.MessageId);
        Assert.Equal("rs1", props.ReplyToSessionId);
    }

    [Fact]
    public void AtomXmlCorrelationFilter_WritesMessageIdAndReplyToSessionId()
    {
        var rule = new RuleEntity { Name = "corr", FilterType = FilterType.CorrelationFilter, MessageId = "m1", ReplyToSessionId = "rs1" };

        var props = AtomXmlReader.ReadRuleProperties(AtomXmlWriter.WriteRuleEntry(rule, "t", "s"));

        Assert.Equal(("m1", "rs1"), (props.MessageId, props.ReplyToSessionId));
    }
}
