using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Tests.Broker;

public class SubscriptionEntityTests
{
    private static BrokeredMessage CreateMessage(string? body = null)
    {
        return new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes(body ?? "hello")
        };
    }

    private static BrokeredMessage CreateMessageWithProps(Dictionary<string, object> props)
    {
        var msg = CreateMessage();
        foreach (var (key, value) in props)
            msg.ApplicationProperties[key] = value;
        return msg;
    }

    [Fact]
    public void HasDefaultRule()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");

        var rule = sub.GetRule("$Default");

        Assert.NotNull(rule);
        Assert.Equal("$Default", rule!.Name);
        Assert.Equal(FilterType.TrueFilter, rule.FilterType);
    }

    [Fact]
    public void AddOrUpdateRule_AddsNewRule()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");
        var rule = new RuleEntity { Name = "MyRule", FilterType = FilterType.SqlFilter, SqlExpression = "1=1" };

        sub.AddOrUpdateRule(rule);

        var retrieved = sub.GetRule("MyRule");
        Assert.NotNull(retrieved);
        Assert.Equal("MyRule", retrieved!.Name);
    }

    [Fact]
    public void RemoveRule_RemovesIt()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");
        var rule = new RuleEntity { Name = "MyRule", FilterType = FilterType.TrueFilter };
        sub.AddOrUpdateRule(rule);

        sub.RemoveRule("MyRule");

        Assert.Null(sub.GetRule("MyRule"));
    }

    [Fact]
    public void DeliverMessage_WithoutForwardTo_EnqueuesInOwnQueue()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");
        var message = CreateMessage("deliver-own");

        sub.DeliverMessage(message);

        var received = sub.Queue.TryDequeueImmediate();
        Assert.NotNull(received);
        Assert.Equal(message.MessageId, received!.MessageId);
    }

    [Fact]
    public void DeliverMessage_WithForwardTo_RoutesToTargetQueue()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic");
        var targetQueue = new QueueEntity("target-queue");
        sub.ForwardTo = "target-queue";
        sub.ResolvedForwardToQueue = targetQueue;

        sub.DeliverMessage(CreateMessage("forward-me"));

        var msgInTarget = targetQueue.TryDequeueImmediate();
        var msgInOwn = sub.Queue.TryDequeueImmediate();

        Assert.NotNull(msgInTarget);
        Assert.Null(msgInOwn);
    }

    // ── SQL filter evaluation tests ───────────────────────────────────────────

    [Theory]
    [InlineData("1=1", true)]
    [InlineData("1=0", false)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    public void SqlFilter_TautologyAndContradiction(string expr, bool expected)
    {
        var rule = new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = expr };
        var msg = CreateMessage();
        Assert.Equal(expected, rule.Matches(msg));
    }

    [Fact]
    public void SqlFilter_NumericEquality_MatchesWhenPropertyEquals()
    {
        var rule = new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = "ClientId = 27" };
        var match = CreateMessageWithProps(new() { ["ClientId"] = 27 });
        var noMatch = CreateMessageWithProps(new() { ["ClientId"] = 99 });

        Assert.True(rule.Matches(match));
        Assert.False(rule.Matches(noMatch));
        Assert.False(rule.Matches(CreateMessage()));
    }

    [Fact]
    public void SqlFilter_StringEquality_MatchesWhenPropertyEquals()
    {
        var rule = new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = "color = 'blue'" };
        var match = CreateMessageWithProps(new() { ["color"] = "blue" });
        var noMatch = CreateMessageWithProps(new() { ["color"] = "red" });

        Assert.True(rule.Matches(match));
        Assert.False(rule.Matches(noMatch));
    }

    [Fact]
    public void SqlFilter_NotExists_TrueWhenPropertyAbsent()
    {
        var rule = new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = "NOT EXISTS(ignore)" };
        var withProp = CreateMessageWithProps(new() { ["ignore"] = "true" });
        var withoutProp = CreateMessage();

        Assert.False(rule.Matches(withProp));
        Assert.True(rule.Matches(withoutProp));
    }

    [Fact]
    public void SqlFilter_Exists_TrueWhenPropertyPresent()
    {
        var rule = new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = "EXISTS(ignore)" };
        var withProp = CreateMessageWithProps(new() { ["ignore"] = "true" });
        var withoutProp = CreateMessage();

        Assert.True(rule.Matches(withProp));
        Assert.False(rule.Matches(withoutProp));
    }

    [Fact]
    public void SqlFilter_Like_MatchesPattern()
    {
        var rule = new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = "name LIKE 'foo%'" };
        var match = CreateMessageWithProps(new() { ["name"] = "foobar" });
        var noMatch = CreateMessageWithProps(new() { ["name"] = "barfoo" });

        Assert.True(rule.Matches(match));
        Assert.False(rule.Matches(noMatch));
    }

    [Fact]
    public void SqlFilter_NotLike_MatchesWhenPatternDoesNotMatch()
    {
        var rule = new RuleEntity { Name = "r", FilterType = FilterType.SqlFilter, SqlExpression = "ignore NOT LIKE 'true'" };
        var matchFalse = CreateMessageWithProps(new() { ["ignore"] = "false" });
        var noMatchTrue = CreateMessageWithProps(new() { ["ignore"] = "true" });

        Assert.True(rule.Matches(matchFalse));
        Assert.False(rule.Matches(noMatchTrue));
    }

    [Fact]
    public void SqlFilter_CompoundOrExpression_WolverineStyleFilter()
    {
        // NOT EXISTS(user.ignore) OR user.ignore NOT LIKE 'true'
        var rule = new RuleEntity
        {
            Name = "r",
            FilterType = FilterType.SqlFilter,
            SqlExpression = "NOT EXISTS(user.ignore) OR user.ignore NOT LIKE 'true'"
        };

        // No 'ignore' property → NOT EXISTS is true → overall true
        Assert.True(rule.Matches(CreateMessage()));

        // ignore = 'false' → NOT EXISTS is false, but NOT LIKE 'true' is true → overall true
        Assert.True(rule.Matches(CreateMessageWithProps(new() { ["ignore"] = "false" })));

        // ignore = 'true' → NOT EXISTS is false, NOT LIKE 'true' is false → overall false
        Assert.False(rule.Matches(CreateMessageWithProps(new() { ["ignore"] = "true" })));
    }
}
