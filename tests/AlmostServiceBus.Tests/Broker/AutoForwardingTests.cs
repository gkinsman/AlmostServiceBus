using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

public class AutoForwardingTests
{
    private static BrokeredMessage Message(string body = "x", string? sessionId = null) => new()
    {
        Body = System.Text.Encoding.UTF8.GetBytes(body),
        SessionId = sessionId
    };

    [Fact]
    public void SubscriptionForwardingToTopic_DeliversToTargetSubscriptions()
    {
        // MassTransit's publish topology: a derived type's topic forwards to its base type's topic.
        var ns = new NamespaceContext("ns");
        var target = ns.CreateSubscription("base-type", "consumer");
        var forwarder = ns.CreateSubscription("derived-type", "base-type", forwardTo: "base-type");

        ns.GetTopic("derived-type")!.Publish(Message("hello"));

        Assert.NotNull(target.Queue.TryDequeueImmediate());
        Assert.Equal(0, forwarder.Queue.MessageCount);
    }

    [Fact]
    public void SubscriptionForwardingToTopic_AppliesTargetSubscriptionRules()
    {
        var ns = new NamespaceContext("ns");
        var matching = ns.CreateSubscription("target", "matching");
        var filtered = ns.CreateSubscription("target", "filtered");
        filtered.RemoveRule("$Default");
        filtered.AddOrUpdateRule(new RuleEntity { Name = "none", FilterType = FilterType.SqlFilter, SqlExpression = "1=0" });
        ns.CreateSubscription("source", "fwd", forwardTo: "target");

        ns.GetTopic("source")!.Publish(Message());

        Assert.Equal(1, matching.Queue.MessageCount);
        Assert.Equal(0, filtered.Queue.MessageCount);
    }

    [Fact]
    public void QueueForwardingToQueue_MovesMessage()
    {
        var ns = new NamespaceContext("ns");
        var target = ns.CreateQueue("target");
        var source = ns.CreateQueue("source");
        source.ForwardTo = "target";

        source.Enqueue(Message());

        Assert.Equal(0, source.MessageCount);
        Assert.Equal(1, target.MessageCount);
    }

    [Fact]
    public void QueueForwardingToTopic_PublishesToSubscriptions()
    {
        var ns = new NamespaceContext("ns");
        var sub = ns.CreateSubscription("target", "sub");
        var source = ns.CreateQueue("source");
        source.ForwardTo = "target";

        source.Enqueue(Message());

        Assert.Equal(0, source.MessageCount);
        Assert.Equal(1, sub.Queue.MessageCount);
    }

    [Fact]
    public void ForwardTarget_CreatedAfterSource_IsResolvedOnDelivery()
    {
        var ns = new NamespaceContext("ns");
        var source = ns.CreateQueue("source");
        source.ForwardTo = "late-topic";
        var sub = ns.CreateSubscription("late-topic", "sub");

        source.Enqueue(Message());

        Assert.Equal(1, sub.Queue.MessageCount);
    }

    [Fact]
    public void ForwardTarget_Deleted_DeadLettersOnSource()
    {
        var ns = new NamespaceContext("ns");
        ns.CreateQueue("target");
        var forwarder = ns.CreateSubscription("source", "fwd", forwardTo: "target");
        ns.DeleteQueue("target");

        ns.GetTopic("source")!.Publish(Message());

        Assert.Equal(0, forwarder.Queue.MessageCount);
        var dead = forwarder.Queue.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dead);
        Assert.StartsWith("The transfer destination entity does not exist.", dead!.DeadLetterReason);
        Assert.Null(dead.DeadLetterErrorDescription);
    }

    [Fact]
    public void ChainOfFourHops_IsDelivered()
    {
        var ns = new NamespaceContext("ns");
        var end = ns.CreateQueue("q4");
        for (var i = 3; i >= 0; i--)
            ns.CreateQueue($"q{i}").ForwardTo = $"q{i + 1}";

        ns.GetQueue("q0")!.Enqueue(Message());

        Assert.Equal(1, end.MessageCount);
    }

    [Fact]
    public void ChainOfFiveHops_DeadLettersOnTheFifthSource()
    {
        var ns = new NamespaceContext("ns");
        var end = ns.CreateQueue("q5");
        for (var i = 4; i >= 0; i--)
            ns.CreateQueue($"q{i}").ForwardTo = $"q{i + 1}";

        ns.GetQueue("q0")!.Enqueue(Message());

        Assert.Equal(0, end.MessageCount);
        var dead = ns.GetQueue("q4")!.DeadLetterQueue.TryDequeueImmediate();
        Assert.NotNull(dead);
        Assert.Equal("Maximum transfer hop count is exceeded.", dead!.DeadLetterReason);
    }

    [Fact]
    public void ForwardingLoop_Terminates()
    {
        var ns = new NamespaceContext("ns");
        var loop = ns.CreateSubscription("topic", "loop");
        loop.ForwardTo = "topic";

        ns.GetTopic("topic")!.Publish(Message());

        Assert.Equal(1, loop.Queue.DeadLetterQueue.MessageCount);
    }

    [Fact]
    public void SessionlessMessage_ForwardedToSessionQueue_DeadLettersOnSource()
    {
        var ns = new NamespaceContext("ns");
        var target = ns.CreateQueue("sessions");
        target.RequiresSession = true;
        var source = ns.CreateQueue("source");
        source.ForwardTo = "sessions";

        source.Enqueue(Message());
        source.Enqueue(Message(sessionId: "s1"));

        Assert.Equal(1, target.MessageCount);
        Assert.StartsWith("The SessionId was not set on a message", source.DeadLetterQueue.TryDequeueImmediate()?.DeadLetterReason);
    }

    [Fact]
    public void ClearingForwardTo_KeepsMessagesInTheSource()
    {
        var ns = new NamespaceContext("ns");
        var target = ns.CreateQueue("target");
        var sub = ns.CreateSubscription("source", "fwd", forwardTo: "target");
        sub.ForwardTo = null;

        ns.GetTopic("source")!.Publish(Message());

        Assert.Equal(1, sub.Queue.MessageCount);
        Assert.Equal(0, target.MessageCount);
    }
}
