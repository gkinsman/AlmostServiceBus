// Ported from Azure.Messaging.ServiceBus.Tests.Administration.ServiceBusManagementClientLiveTests
using Azure.Messaging.ServiceBus.Administration;

namespace AlmostServiceBus.SdkLive.Tests;

public class AdminClientLiveTests : SdkLiveTestBase
{
    [Fact]
    public async Task CreateDeleteQueue()
    {
        var queueName = $"sdk-admin-{Guid.NewGuid():N}"[..24];
        var props = await AdminClient.CreateQueueAsync(queueName);
        Assert.Equal(queueName, props.Value.Name);

        Assert.True(await AdminClient.QueueExistsAsync(queueName));
        await AdminClient.DeleteQueueAsync(queueName);
        Assert.False(await AdminClient.QueueExistsAsync(queueName));
    }

    [Fact]
    public async Task CreateQueueWithOptions()
    {
        var queueName = $"sdk-admin-{Guid.NewGuid():N}"[..24];
        var options = new CreateQueueOptions(queueName)
        {
            LockDuration = TimeSpan.FromSeconds(45),
            RequiresSession = true,
            DefaultMessageTimeToLive = TimeSpan.FromMinutes(10),
            MaxDeliveryCount = 5,
        };

        var props = await AdminClient.CreateQueueAsync(options);
        Assert.Equal(queueName, props.Value.Name);
        Assert.True(props.Value.RequiresSession);

        await AdminClient.DeleteQueueAsync(queueName);
    }

    [Fact]
    public async Task GetQueue()
    {
        var queueName = await CreateQueueAsync();
        var props = await AdminClient.GetQueueAsync(queueName);
        Assert.Equal(queueName, props.Value.Name);
    }

    [Fact]
    public async Task QueueExists_ReturnsFalseForNonexistent()
    {
        Assert.False(await AdminClient.QueueExistsAsync("nonexistent-queue-name"));
    }

    [Fact]
    public async Task CreateDeleteTopic()
    {
        var topicName = $"sdk-admin-{Guid.NewGuid():N}"[..24];
        var props = await AdminClient.CreateTopicAsync(topicName);
        Assert.Equal(topicName, props.Value.Name);

        Assert.True(await AdminClient.TopicExistsAsync(topicName));
        await AdminClient.DeleteTopicAsync(topicName);
        Assert.False(await AdminClient.TopicExistsAsync(topicName));
    }

    [Fact]
    public async Task CreateDeleteSubscription()
    {
        var (topicName, _) = await CreateTopicAsync(subscriptions: []);
        var subName = "test-sub";

        var props = await AdminClient.CreateSubscriptionAsync(topicName, subName);
        Assert.Equal(subName, props.Value.SubscriptionName);

        Assert.True(await AdminClient.SubscriptionExistsAsync(topicName, subName));
        await AdminClient.DeleteSubscriptionAsync(topicName, subName);
        Assert.False(await AdminClient.SubscriptionExistsAsync(topicName, subName));
    }

    [Fact]
    public async Task GetTopic()
    {
        var (topicName, _) = await CreateTopicAsync();
        var props = await AdminClient.GetTopicAsync(topicName);
        Assert.Equal(topicName, props.Value.Name);
    }

    [Fact]
    public async Task GetSubscription()
    {
        var (topicName, subs) = await CreateTopicAsync();
        var props = await AdminClient.GetSubscriptionAsync(topicName, subs[0]);
        Assert.Equal(subs[0], props.Value.SubscriptionName);
    }

    [Fact]
    public async Task ListQueues()
    {
        var queueName1 = await CreateQueueAsync();
        var queueName2 = await CreateQueueAsync();

        var queues = new List<string>();
        await foreach (var q in AdminClient.GetQueuesAsync())
            queues.Add(q.Name);

        Assert.Contains(queueName1, queues);
        Assert.Contains(queueName2, queues);
    }

    [Fact]
    public async Task ListTopics()
    {
        var (topicName1, _) = await CreateTopicAsync();
        var (topicName2, _) = await CreateTopicAsync();

        var topics = new List<string>();
        await foreach (var t in AdminClient.GetTopicsAsync())
            topics.Add(t.Name);

        Assert.Contains(topicName1, topics);
        Assert.Contains(topicName2, topics);
    }
}
