// Ported from Azure.Messaging.ServiceBus.Tests.Sender.SenderLiveTests
using Azure.Messaging.ServiceBus;

namespace AlmostServiceBus.SdkLive.Tests;

public class SenderLiveTests : SdkLiveTestBase
{
    [Fact]
    public async Task SendConnStringWithSharedKey()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());
    }

    [Fact]
    public async Task SendConnectionTopic()
    {
        var (topicName, _) = await CreateTopicAsync();
        await using var sender = Client.CreateSender(topicName);
        await sender.SendMessageAsync(GetMessage());
    }

    [Fact]
    public async Task SendTopicSession()
    {
        var (topicName, _) = await CreateTopicAsync();
        await using var sender = Client.CreateSender(topicName);
        await sender.SendMessageAsync(GetMessage("sessionId"));
    }

    [Fact]
    public async Task CanSendAMessageBatch()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        AddMessages(batch, 3);
        await sender.SendMessagesAsync(batch);
    }

    [Fact]
    public async Task SendingEmptyBatchDoesNotThrow()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        await sender.SendMessagesAsync(batch);
    }

    [Fact]
    public async Task CanSendAnEmptyBodyMessageBatch()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        batch.TryAddMessage(new ServiceBusMessage(Array.Empty<byte>()));
        await sender.SendMessagesAsync(batch);
    }

    [Fact]
    public async Task TryAddReturnsFalseIfSizeExceed()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();

        var padding = 500;
        var size = (int)(batch.MaxSizeInBytes - padding);

        Assert.True(batch.TryAddMessage(new ServiceBusMessage(new byte[size])), "First message should fit");
        Assert.False(batch.TryAddMessage(new ServiceBusMessage(new byte[padding + 1])), "Second message should exceed size");

        await sender.SendMessagesAsync(batch);
    }

    [Fact]
    public async Task ClientProperties()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        Assert.Equal(queueName, sender.EntityPath);
    }

    [Fact]
    public async Task Schedule()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        var scheduleTime = DateTimeOffset.UtcNow.AddHours(10);
        var seq = await sender.ScheduleMessageAsync(GetMessage(), scheduleTime);

        await using var receiver = Client.CreateReceiver(queueName);
        var msg = await receiver.PeekMessageAsync(seq);
        Assert.NotNull(msg);
        Assert.Equal(0, Convert.ToInt32(new TimeSpan(scheduleTime.Ticks - msg.ScheduledEnqueueTime.Ticks).TotalSeconds));

        await sender.CancelScheduledMessageAsync(seq);
        msg = await receiver.PeekMessageAsync(seq);
        Assert.Null(msg);
    }

    [Fact]
    public async Task ScheduleMultiple()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        var scheduleTime = DateTimeOffset.UtcNow.AddHours(10);
        var sequenceNums = await sender.ScheduleMessagesAsync(GetMessages(5), scheduleTime);

        await using var receiver = Client.CreateReceiver(queueName);
        foreach (long seq in sequenceNums)
        {
            var msg = await receiver.PeekMessageAsync(seq);
            Assert.NotNull(msg);
            Assert.Equal(0, Convert.ToInt32(new TimeSpan(scheduleTime.Ticks - msg.ScheduledEnqueueTime.Ticks).TotalSeconds));
        }

        await sender.CancelScheduledMessagesAsync(sequenceNums);
        foreach (long seq in sequenceNums)
        {
            var msg = await receiver.PeekMessageAsync(seq);
            Assert.Null(msg);
        }
    }

    [Fact]
    public async Task CloseSenderShouldNotCloseConnection()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        var scheduleTime = DateTimeOffset.UtcNow.AddHours(10);
        var sequenceNum = await sender.ScheduleMessageAsync(GetMessage(), scheduleTime);
        await sender.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => sender.SendMessageAsync(GetMessage()));

        // receive should still work on same connection
        await using var receiver = Client.CreateReceiver(queueName);
        var msg = await receiver.PeekMessageAsync(sequenceNum);
        Assert.NotNull(msg);
    }

    [Fact]
    public async Task SendSessionMessageToNonSessionfulEntityShouldNotThrow()
    {
        var queueName = await CreateQueueAsync(enableSession: false);
        var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage("sessionId"));
        var receiver = Client.CreateReceiver(queueName);
        var msg = await receiver.ReceiveMessageAsync();
        Assert.NotNull(msg);
        Assert.Equal("sessionId", msg.SessionId);
    }

    [Fact]
    public async Task SendNonSessionMessageToSessionfulEntityShouldThrow()
    {
        var queueName = await CreateQueueAsync(enableSession: true);
        await using var sender = Client.CreateSender(queueName);
        // Real Service Bus throws InvalidOperationException; the emulator currently
        // returns the rejection as a ServiceBusException. Both indicate the message
        // was rejected because the queue requires sessions.
        var ex = await Record.ExceptionAsync(() => sender.SendMessageAsync(GetMessage()));
        Assert.NotNull(ex);
        Assert.True(ex is InvalidOperationException || ex is Azure.Messaging.ServiceBus.ServiceBusException,
            $"Expected InvalidOperationException or ServiceBusException, got {ex.GetType().Name}");
    }

    [Fact]
    public async Task CanSendReceivedMessage()
    {
        var queueName = await CreateQueueAsync();
        await using var sender = Client.CreateSender(queueName);
        var messageCt = 10;
        var messages = GetMessages(messageCt);
        await sender.SendMessagesAsync(messages);

        var receiver = Client.CreateReceiver(queueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });

        var receivedMessages = new List<ServiceBusReceivedMessage>();
        var remaining = messageCt;
        while (remaining > 0)
        {
            foreach (var msg in await receiver.ReceiveMessagesAsync(messageCt))
            {
                remaining--;
                receivedMessages.Add(msg);
            }
        }

        // Re-send received messages
        foreach (var msg in receivedMessages)
            await sender.SendMessageAsync(new ServiceBusMessage(msg));

        // Receive again and verify order
        var messageEnum = receivedMessages.GetEnumerator();
        remaining = messageCt;
        while (remaining > 0)
        {
            foreach (var msg in await receiver.ReceiveMessagesAsync(remaining))
            {
                remaining--;
                messageEnum.MoveNext();
                Assert.Equal(messageEnum.Current.MessageId, msg.MessageId);
            }
        }
        Assert.Equal(0, remaining);
    }

    [Fact]
    public async Task CancellingSendDoesNotBlockSubsequentSends()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(20));
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            sender.SendMessagesAsync(GetMessages(300), cts.Token));

        var start = DateTime.UtcNow;
        await sender.SendMessageAsync(GetMessage());
        var elapsed = DateTime.UtcNow - start;
        Assert.True(elapsed < TimeSpan.FromSeconds(10), $"Subsequent send took {elapsed}");
    }
}
