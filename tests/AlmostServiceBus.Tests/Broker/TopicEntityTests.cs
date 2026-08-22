using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

public class TopicEntityTests
{
    private static BrokeredMessage CreateMessage(string? body = null)
    {
        return new BrokeredMessage
        {
            Body = System.Text.Encoding.UTF8.GetBytes(body ?? "hello")
        };
    }

    [Fact]
    public void Properties_HaveDefaults()
    {
        var topic = new TopicEntity("my-topic");

        Assert.Equal("my-topic", topic.Name);
        Assert.Equal(1024L, topic.MaxSizeInMegabytes);
        Assert.Equal(TimeSpan.MaxValue, topic.DefaultMessageTimeToLive);
        Assert.False(topic.EnablePartitioning);
        Assert.False(topic.EnableExpress);
        Assert.True(topic.EnableBatchedOperations);
        Assert.Null(topic.UserMetadata);
    }

    [Fact]
    public void AddSubscription_ReturnsExistingIfAlreadyExists()
    {
        var topic = new TopicEntity("my-topic");

        var sub1 = topic.AddSubscription("sub1");
        var sub2 = topic.AddSubscription("sub1");

        Assert.Same(sub1, sub2);
    }

    [Fact]
    public void GetSubscription_ReturnsNullIfNotFound()
    {
        var topic = new TopicEntity("my-topic");

        var result = topic.GetSubscription("nonexistent");

        Assert.Null(result);
    }

    [Fact]
    public void RemoveSubscription_RemovesIt()
    {
        var topic = new TopicEntity("my-topic");
        topic.AddSubscription("sub1");

        topic.RemoveSubscription("sub1");

        Assert.Null(topic.GetSubscription("sub1"));
    }

    [Fact]
    public void Publish_FansOutToAllSubscriptions()
    {
        var topic = new TopicEntity("my-topic");
        var subA = topic.AddSubscription("subA");
        var subB = topic.AddSubscription("subB");

        topic.Publish(CreateMessage("fan-out"));

        var msgA = subA.Queue.TryDequeueImmediate();
        var msgB = subB.Queue.TryDequeueImmediate();

        Assert.NotNull(msgA);
        Assert.NotNull(msgB);
    }

    [Fact]
    public void Publish_ClonesMessagePerSubscription()
    {
        var topic = new TopicEntity("my-topic");
        var subA = topic.AddSubscription("subA");
        var subB = topic.AddSubscription("subB");

        topic.Publish(CreateMessage("clone-test"));

        var msgA = subA.Queue.TryDequeueImmediate();
        var msgB = subB.Queue.TryDequeueImmediate();

        Assert.NotNull(msgA);
        Assert.NotNull(msgB);
        Assert.NotSame(msgA, msgB);
    }

    [Fact]
    public void Publish_WithForwardTo_RoutesToTargetQueue()
    {
        var topic = new TopicEntity("my-topic");
        var targetQueue = new QueueEntity("target-queue");
        var sub = topic.AddSubscription("sub1");
        sub.ForwardTo = "target-queue";
        sub.ResolvedForwardToQueue = targetQueue;

        topic.Publish(CreateMessage("forwarded"));

        var msgInTarget = targetQueue.TryDequeueImmediate();
        var msgInOwn = sub.Queue.TryDequeueImmediate();

        Assert.NotNull(msgInTarget);
        Assert.Null(msgInOwn);
    }
}
