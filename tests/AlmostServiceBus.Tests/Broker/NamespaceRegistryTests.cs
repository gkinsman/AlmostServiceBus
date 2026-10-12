using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

public class NamespaceRegistryTests
{
    // ── NamespaceRegistry tests ──────────────────────────────────────────────

    [Fact]
    public void GetOrCreate_ReturnsSameContextForSameNamespace()
    {
        var registry = new NamespaceRegistry();

        var ctx1 = registry.GetOrCreate("my-namespace");
        var ctx2 = registry.GetOrCreate("my-namespace");

        Assert.Same(ctx1, ctx2);
    }

    [Fact]
    public void GetOrCreate_ReturnsDifferentContextsForDifferentNamespaces()
    {
        var registry = new NamespaceRegistry();

        var ctx1 = registry.GetOrCreate("namespace-a");
        var ctx2 = registry.GetOrCreate("namespace-b");

        Assert.NotSame(ctx1, ctx2);
    }

    [Fact]
    public void NamespaceIsolation_NoMessageCrossContamination()
    {
        var registry = new NamespaceRegistry();

        var ns1 = registry.GetOrCreate("tenant-1");
        var ns2 = registry.GetOrCreate("tenant-2");

        var q1 = ns1.CreateQueue("shared-name");
        var q2 = ns2.CreateQueue("shared-name");

        q1.Enqueue(new BrokeredMessage { Body = System.Text.Encoding.UTF8.GetBytes("hello") });

        // ns2 queue should be empty
        var msg = q2.TryDequeueImmediate();
        Assert.Null(msg);
    }

    // ── NamespaceContext — queue tests ──────────────────────────────────────

    [Fact]
    public void NamespaceContext_CreateQueue_ReturnsQueue()
    {
        var ctx = new NamespaceContext("test-ns");

        var queue = ctx.CreateQueue("my-queue");

        Assert.NotNull(queue);
        Assert.Equal("my-queue", queue.Name);
    }

    [Fact]
    public void NamespaceContext_CreateQueue_Idempotent()
    {
        var ctx = new NamespaceContext("test-ns");

        var q1 = ctx.CreateQueue("my-queue");
        var q2 = ctx.CreateQueue("my-queue");

        Assert.Same(q1, q2);
    }

    [Fact]
    public void NamespaceContext_GetQueue_ReturnsNullIfNotFound()
    {
        var ctx = new NamespaceContext("test-ns");

        var result = ctx.GetQueue("nonexistent");

        Assert.Null(result);
    }

    // ── NamespaceContext — topic tests ──────────────────────────────────────

    [Fact]
    public void NamespaceContext_CreateTopic_ReturnsTopic()
    {
        var ctx = new NamespaceContext("test-ns");

        var topic = ctx.CreateTopic("my-topic");

        Assert.NotNull(topic);
        Assert.Equal("my-topic", topic.Name);
    }

    [Fact]
    public void NamespaceContext_GetTopic_ReturnsNullIfNotFound()
    {
        var ctx = new NamespaceContext("test-ns");

        var result = ctx.GetTopic("nonexistent");

        Assert.Null(result);
    }

    // ── NamespaceContext — subscription / ForwardTo ─────────────────────────

    [Fact]
    public void NamespaceContext_CreateSubscription_LinksForwardTo()
    {
        var ctx = new NamespaceContext("test-ns");

        ctx.CreateTopic("orders");
        ctx.CreateQueue("target-queue");
        var sub = ctx.CreateSubscription("orders", "sub-1", forwardTo: "target-queue");

        Assert.NotNull(sub);
        Assert.Equal("target-queue", sub.ForwardTo);
        Assert.Null(ctx.GetTopic("target-queue"));
    }

    [Fact]
    public void NamespaceContext_CreateSubscription_ForwardToExistingTopic_DoesNotCreateQueue()
    {
        var ctx = new NamespaceContext("test-ns");
        ctx.CreateTopic("target-topic");

        ctx.CreateSubscription("orders", "sub-1", forwardTo: "target-topic");

        Assert.Null(ctx.GetQueue("target-topic"));
    }

    [Fact]
    public void NamespaceContext_CreateSubscription_ForwardToMissingEntity_CreatesQueue()
    {
        var ctx = new NamespaceContext("test-ns");

        ctx.CreateSubscription("orders", "sub-1", forwardTo: "target-queue");

        Assert.NotNull(ctx.GetQueue("target-queue"));
    }

    // ── NamespaceContext — sequence numbers ─────────────────────────────────

    [Fact]
    public void NamespaceContext_NextSequenceNumber_Increments()
    {
        var ctx = new NamespaceContext("test-ns");

        var first = ctx.NextSequenceNumber();
        var second = ctx.NextSequenceNumber();

        Assert.Equal(1L, first);
        Assert.Equal(2L, second);
    }

    // ── NamespaceContext — address resolution ───────────────────────────────

    [Fact]
    public void NamespaceContext_ResolveEntity_FindsQueueOrSubscription()
    {
        var ctx = new NamespaceContext("test-ns");

        ctx.CreateQueue("my-queue");
        ctx.CreateSubscription("my-topic", "sub-1");

        // Direct queue resolve
        var directQueue = ctx.ResolveQueue("my-queue");
        Assert.NotNull(directQueue);
        Assert.Equal("my-queue", directQueue!.Name);

        // Subscription resolve via topicName/Subscriptions/subName
        var subQueue = ctx.ResolveQueue("my-topic/Subscriptions/sub-1");
        Assert.NotNull(subQueue);
        Assert.Equal("my-topic/Subscriptions/sub-1", subQueue!.Name);

        // Non-existent returns null
        var missing = ctx.ResolveQueue("does-not-exist");
        Assert.Null(missing);
    }
}
