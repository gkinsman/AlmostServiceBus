using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Tests.Broker;

/// <summary>How a subscription turns matching rules into delivered copies, as observed on Azure (decisions log Q9).</summary>
public class RuleDeliveryTests
{
    private static BrokeredMessage Red() => new() { ApplicationProperties = { ["color"] = "red" } };

    private static RuleEntity Sql(string name, string filter, string? action = null) => new()
    {
        Name = name, FilterType = FilterType.SqlFilter, SqlExpression = filter, ActionExpression = action
    };

    private static RuleEntity True(string name, string? action = null) => new()
    {
        Name = name, FilterType = FilterType.TrueFilter, ActionExpression = action
    };

    private static SubscriptionEntity Subscription(params RuleEntity[] rules)
    {
        var sub = new SubscriptionEntity("sub", "topic");
        sub.RemoveRule("$Default");
        foreach (var rule in rules) sub.AddOrUpdateRule(rule);
        return sub;
    }

    private static List<BrokeredMessage> Drain(QueueEntity queue)
    {
        var messages = new List<BrokeredMessage>();
        while (queue.TryDequeueImmediate() is { } m) messages.Add(m);
        return messages;
    }

    [Fact]
    public void TwoActionRules_TwoCopies_EachWithOwnRuleNameAndEffect()
    {
        var sub = Subscription(True("r0", "SET user.r1 = '1'"), True("r1", "SET user.r2 = '2'"));

        sub.DeliverMessage(Red());

        var copies = Drain(sub.Queue).OrderBy(m => m.ApplicationProperties["RuleName"]).ToList();
        Assert.Equal(2, copies.Count);
        Assert.Equal("r0", copies[0].ApplicationProperties["RuleName"]);
        Assert.Equal("1", copies[0].ApplicationProperties["r1"]);
        Assert.False(copies[0].ApplicationProperties.ContainsKey("r2"));
        Assert.Equal("r1", copies[1].ApplicationProperties["RuleName"]);
        Assert.Equal("2", copies[1].ApplicationProperties["r2"]);
        Assert.False(copies[1].ApplicationProperties.ContainsKey("r1"));
    }

    [Fact]
    public void ActionRuleAndPlainRule_TwoCopies_PlainHasNoRuleName()
    {
        var sub = Subscription(True("r0", "SET user.r1 = '1'"), True("r1"));

        sub.DeliverMessage(Red());

        var copies = Drain(sub.Queue);
        Assert.Equal(2, copies.Count);
        Assert.Single(copies, m => m.ApplicationProperties.ContainsKey("RuleName"));
        Assert.Single(copies, m => !m.ApplicationProperties.ContainsKey("RuleName") && !m.ApplicationProperties.ContainsKey("r1"));
    }

    [Fact]
    public void TwoPlainRules_OneCopy()
    {
        var sub = Subscription(True("r0"), Sql("r1", "1 = 1"));

        sub.DeliverMessage(Red());

        Assert.Single(Drain(sub.Queue));
    }

    [Fact]
    public void NonMatchingActionRuleAndMatchingPlainRule_OnePlainCopy()
    {
        var sub = Subscription(Sql("r0", "1 = 0", "SET user.r1 = '1'"), True("r1"));

        sub.DeliverMessage(Red());

        var copy = Assert.Single(Drain(sub.Queue));
        Assert.False(copy.ApplicationProperties.ContainsKey("RuleName"));
    }

    [Fact]
    public void FilterError_DeadLettersOneCopy_OtherRulesStillDeliver()
    {
        var sub = Subscription(Sql("r0", "color > 1"), True("r1"));

        sub.DeliverMessage(Red());

        Assert.Single(Drain(sub.Queue));
        var dead = Assert.Single(Drain(sub.Queue.DeadLetterQueue));
        Assert.Equal("FilterException", dead.DeadLetterReason);
        Assert.Equal("Operator '>' cannot be applied to operands of type 'string' and 'long'", dead.DeadLetterErrorDescription);
        Assert.Null(dead.DeadLetterSource); // the annotation is only for messages forwarded out of a dead-letter queue
        Assert.Equal("FilteringEngine", dead.ApplicationProperties["DeadLetterSource"]);
        Assert.False(dead.ApplicationProperties.ContainsKey("RuleName"));
    }

    [Fact]
    public void ActionError_DeadLettersPartialCopy()
    {
        var sub = Subscription(True("r0", "SET user.a = 1; SET user.b = 1/0"));

        sub.DeliverMessage(Red());

        Assert.Empty(Drain(sub.Queue));
        var dead = Assert.Single(Drain(sub.Queue.DeadLetterQueue));
        Assert.Equal("RuleActionException", dead.DeadLetterReason);
        Assert.Equal("Attempted to divide by zero.", dead.DeadLetterErrorDescription);
        Assert.Equal(1L, dead.ApplicationProperties["a"]);
        Assert.Equal("r0", dead.ApplicationProperties["RuleName"]);
    }

    [Fact]
    public void ActionErrorPlusOtherActionRule_OneDeadLetteredOneDelivered()
    {
        var sub = Subscription(True("r0", "SET user.x = 1/0"), True("r1", "SET user.y = 1"));

        sub.DeliverMessage(Red());

        var delivered = Assert.Single(Drain(sub.Queue));
        Assert.Equal("r1", delivered.ApplicationProperties["RuleName"]);
        Assert.Single(Drain(sub.Queue.DeadLetterQueue));
    }

    [Fact]
    public void ErrorsWithDeadLetteringDisabled_AreDropped()
    {
        var sub = Subscription(Sql("r0", "color > 1"), True("r1", "SET user.x = 1/0"));
        sub.DeadLetteringOnFilterEvaluationExceptions = false;

        sub.DeliverMessage(Red());

        Assert.Empty(Drain(sub.Queue));
        Assert.Empty(Drain(sub.Queue.DeadLetterQueue));
    }

    [Fact]
    public void ActionOnForwardingSubscription_TargetQueueReceivesModifiedCopy()
    {
        var ns = new NamespaceContext("ns");
        var target = ns.CreateQueue("target");
        var forwarder = ns.CreateSubscription("topic", "fwd", forwardTo: "target");
        forwarder.RemoveRule("$Default");
        forwarder.AddOrUpdateRule(True("tag", "SET sys.Label = 'fwd'"));

        ns.GetTopic("topic")!.Publish(Red());

        var forwarded = target.TryDequeueImmediate();
        Assert.NotNull(forwarded);
        Assert.Equal("fwd", forwarded.Subject);
    }

    [Fact]
    public void ActionNeverTouchesOriginalOrOtherSubscriptions()
    {
        var topic = new TopicEntity("topic");
        var acting = topic.AddSubscription("acting");
        acting.RemoveRule("$Default");
        acting.AddOrUpdateRule(True("tag", "SET user.tag = 'A'"));
        var plain = topic.AddSubscription("plain");
        var original = Red();

        topic.Publish(original);

        Assert.Equal("A", acting.Queue.TryDequeueImmediate()!.ApplicationProperties["tag"]);
        Assert.False(plain.Queue.TryDequeueImmediate()!.ApplicationProperties.ContainsKey("tag"));
        Assert.False(original.ApplicationProperties.ContainsKey("tag"));
    }

    [Fact]
    public void SequenceNumber_IsAssignedBeforeFiltersRun()
    {
        var sub = Subscription(Sql("r0", "sys.SequenceNumber > 0"));

        sub.DeliverMessage(Red());

        var copy = Assert.Single(Drain(sub.Queue));
        Assert.True(copy.SequenceNumber > 0);
    }

    [Fact]
    public void ForwardedCopy_GetsTheTargetQueuesOwnSequenceNumber()
    {
        var ns = new NamespaceContext("ns");
        var target = ns.CreateQueue("target");
        ns.CreateSubscription("topic", "fwd", forwardTo: "target");
        target.Enqueue(new BrokeredMessage());

        ns.GetTopic("topic")!.Publish(Red());

        var first = target.TryDequeueImmediate()!;
        var forwarded = target.TryDequeueImmediate()!;
        Assert.NotEqual(first.SequenceNumber, forwarded.SequenceNumber);
    }

    [Fact]
    public void AddOrUpdateRule_InvalidFilter_Throws_RuleAbsent()
    {
        var sub = new SubscriptionEntity("sub", "topic");

        Assert.Throws<SqlSyntaxException>(() => sub.AddOrUpdateRule(Sql("bad", "color IN ('a'")));
        Assert.Null(sub.GetRule("bad"));
    }

    [Fact]
    public void AddOrUpdateRule_InvalidAction_Throws()
    {
        var sub = new SubscriptionEntity("sub", "topic");

        Assert.Throws<SqlSyntaxException>(() => sub.AddOrUpdateRule(True("bad", "SET")));
        Assert.Null(sub.GetRule("bad"));
    }

    [Fact]
    public void AddOrUpdateRule_WhitespaceSqlFilter_Throws()
    {
        var sub = new SubscriptionEntity("sub", "topic");
        Assert.Throws<SqlSyntaxException>(() => sub.AddOrUpdateRule(Sql("bad", " ")));
    }

    [Fact]
    public void Matches_OnNeverValidatedInvalidRule_Throws() =>
        Assert.Throws<SqlSyntaxException>(() => Sql("bad", "color IN ('a'").Matches(Red()));

    [Fact]
    public async Task ConcurrentRuleReplaceDuringPublish_NeverThrows()
    {
        var sub = Subscription(Sql("r", "a = 1"));
        var message = new BrokeredMessage { ApplicationProperties = { ["a"] = 1L } };
        Exception? failure = null;

        var writer = Task.Run(() =>
        {
            try
            {
                for (var i = 0; i < 10_000; i++)
                    sub.AddOrUpdateRule(Sql("r", i % 2 == 0 ? "a = 2" : "a = 1"));
            }
            catch (Exception ex) { failure = ex; }
        });
        var reader = Task.Run(() =>
        {
            try
            {
                for (var i = 0; i < 10_000; i++)
                    sub.DeliverMessage(message.Clone());
            }
            catch (Exception ex) { failure = ex; }
        });
        await Task.WhenAll(writer, reader);

        Assert.Null(failure);
        Assert.Empty(Drain(sub.Queue.DeadLetterQueue));
    }

    [Fact]
    public async Task ConcurrentMutationOfOneRuleInstance_NeverEvaluatesATornRule()
    {
        var rule = Sql("r", "a = 1", "SET user.x = 1");
        var sub = Subscription(rule);
        var message = new BrokeredMessage { ApplicationProperties = { ["a"] = 1L } };
        Exception? failure = null;

        var writer = Task.Run(() =>
        {
            for (var i = 0; i < 10_000; i++)
            {
                rule.SqlExpression = i % 2 == 0 ? "a = 2" : "a = 1";
                rule.ActionExpression = i % 2 == 0 ? "SET user.y = 2" : "SET user.x = 1";
            }
        });
        var reader = Task.Run(() =>
        {
            try
            {
                for (var i = 0; i < 10_000; i++)
                    sub.DeliverMessage(message.Clone());
            }
            catch (Exception ex) { failure = ex; }
        });
        await Task.WhenAll(writer, reader);

        Assert.Null(failure);
    }
}
