using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

/// <summary>
/// A subscription's own queue must stream dashboard events under the path the dashboard uses
/// for it, whichever way the subscription was created. Before this, subscription queues were
/// never connected to the event bus, so the subscription Messages / Dead Letter views only
/// updated on refresh.
/// </summary>
public class SubscriptionEventTests
{
    private static BrokeredMessage Message() => new() { Body = System.Text.Encoding.UTF8.GetBytes("x") };

    [Fact]
    public void SubscriptionCreatedThroughContext_PublishesUnderSubscriptionPath()
    {
        var bus = new MessageEventBus();
        var reader = bus.Subscribe();
        var ns = new NamespaceContext("ns", bus);

        ns.CreateSubscription("orders", "billing");
        ns.GetTopic("orders")!.Publish(Message());

        Assert.True(reader.TryRead(out var evt));
        Assert.Equal(MessageEventType.Enqueued, evt.Type);
        Assert.Equal("ns", evt.Namespace);
        Assert.Equal("orders/subscriptions/billing", evt.Entity);
    }

    [Fact]
    public void SubscriptionAddedOnAnExistingTopic_PublishesToo()
    {
        // The management API adds subscriptions straight on the topic it resolved.
        var bus = new MessageEventBus();
        var reader = bus.Subscribe();
        var ns = new NamespaceContext("ns", bus);

        var topic = ns.CreateTopic("orders");
        var sub = topic.AddSubscription("audit");
        topic.Publish(Message());

        Assert.True(reader.TryRead(out var enqueued));
        Assert.Equal("orders/subscriptions/audit", enqueued.Entity);

        var m = sub.Queue.TryDequeueImmediate()!;
        sub.Queue.DeadLetter(m.LockToken!, "r", "d");
        Assert.True(reader.TryRead(out var dead));
        Assert.Equal(MessageEventType.DeadLettered, dead.Type);
        Assert.Equal("orders/subscriptions/audit", dead.Entity);
    }

    [Fact]
    public void ForwardingSubscription_PublishesUnderTheTargetQueue()
    {
        var bus = new MessageEventBus();
        var reader = bus.Subscribe();
        var ns = new NamespaceContext("ns", bus);

        ns.CreateSubscription("orders", "worker", forwardTo: "orders-worker");
        ns.GetTopic("orders")!.Publish(Message());

        Assert.True(reader.TryRead(out var evt));
        Assert.Equal("orders-worker", evt.Entity);
    }

    [Fact]
    public void WithoutAnEventBus_NothingIsPublishedAndNothingThrows()
    {
        var ns = new NamespaceContext("ns");
        ns.CreateSubscription("orders", "billing");
        ns.GetTopic("orders")!.Publish(Message());
        Assert.Equal(1, ns.GetSubscription("orders", "billing")!.Queue.MessageCount);
    }
}
