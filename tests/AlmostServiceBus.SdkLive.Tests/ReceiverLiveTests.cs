// Ported from Azure.Messaging.ServiceBus.Tests.Receiver.ReceiverLiveTests
using Azure.Messaging.ServiceBus;

namespace AlmostServiceBus.SdkLive.Tests;

public class ReceiverLiveTests : SdkLiveTestBase
{
    [Fact]
    public async Task PeekMessages()
    {
        var queueName = await CreateQueueAsync();
        var messageCt = 10;

        var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        var sentMessages = AddAndReturnMessages(batch, messageCt);
        await sender.SendMessagesAsync(batch);

        await using var receiver = Client.CreateReceiver(queueName);
        var messageEnum = sentMessages.GetEnumerator();
        var ct = 0;
        while (ct < messageCt)
        {
            foreach (var peeked in await receiver.PeekMessagesAsync(maxMessages: messageCt))
            {
                messageEnum.MoveNext();
                Assert.Equal(messageEnum.Current.MessageId, peeked.MessageId);
                ct++;
            }
        }
        Assert.Equal(messageCt, ct);
    }

    [Fact]
    public async Task PeekSingleMessage()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        var msgs = GetMessages(2);
        await sender.SendMessagesAsync(msgs);

        var receiver = Client.CreateReceiver(queueName);
        var message1 = await receiver.PeekMessageAsync();
        Assert.NotNull(message1);
        Assert.True(message1.SequenceNumber > 0);
        var message2 = await receiver.PeekMessageAsync(message1.SequenceNumber + 1);
        Assert.NotNull(message2);
        Assert.Equal(msgs[1].MessageId, message2.MessageId);
    }

    [Fact]
    public async Task ReceiveMessagesInPeekLockMode()
    {
        var queueName = await CreateQueueAsync();
        var messageCount = 10;

        var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        var messages = AddAndReturnMessages(batch, messageCount);
        await sender.SendMessagesAsync(batch);

        var receiver = Client.CreateReceiver(queueName);
        var messageEnum = messages.GetEnumerator();
        var remaining = messageCount;
        while (remaining > 0)
        {
            foreach (var item in await receiver.ReceiveMessagesAsync(remaining))
            {
                remaining--;
                messageEnum.MoveNext();
                Assert.Equal(messageEnum.Current.MessageId, item.MessageId);
                Assert.Equal(1, item.DeliveryCount);
            }
        }
        Assert.Equal(0, remaining);

        // Messages should still be peek-able (locked but not completed)
        var peeked = await receiver.PeekMessagesAsync(messageCount);
        for (int i = 0; i < peeked.Count; i++)
        {
            Assert.Equal(messages[i].MessageId, peeked[i].MessageId);
        }
    }

    [Fact]
    public async Task ReceiveMessagesInReceiveAndDeleteMode()
    {
        var queueName = await CreateQueueAsync();
        var messageCount = 10;

        var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        var messages = AddAndReturnMessages(batch, messageCount);
        await sender.SendMessagesAsync(batch);

        var receiver = Client.CreateReceiver(queueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        var messageEnum = messages.GetEnumerator();
        var remaining = messageCount;
        while (remaining > 0)
        {
            foreach (var item in await receiver.ReceiveMessagesAsync(remaining))
            {
                messageEnum.MoveNext();
                Assert.Equal(messageEnum.Current.MessageId, item.MessageId);
                remaining--;
            }
        }
        Assert.Equal(0, remaining);

        var peeked = await receiver.PeekMessageAsync();
        Assert.Null(peeked);
    }

    [Fact]
    public async Task ReceiveSingleMessageInReceiveAndDeleteMode()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        var sentMessage = GetMessage();
        await sender.SendMessageAsync(sentMessage);

        var receiver = Client.CreateReceiver(queueName, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
        });
        var received = await receiver.ReceiveMessageAsync();
        Assert.NotNull(received);
        Assert.Equal(sentMessage.MessageId, received.MessageId);

        var peeked = await receiver.PeekMessageAsync();
        Assert.Null(peeked);
    }

    [Fact]
    public async Task CompleteMessages()
    {
        var queueName = await CreateQueueAsync();
        var messageCount = 10;

        var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        var messages = AddAndReturnMessages(batch, messageCount);
        await sender.SendMessagesAsync(batch);

        var receiver = Client.CreateReceiver(queueName);
        var messageEnum = messages.GetEnumerator();
        var remaining = messageCount;
        while (remaining > 0)
        {
            foreach (var item in await receiver.ReceiveMessagesAsync(remaining))
            {
                remaining--;
                messageEnum.MoveNext();
                Assert.Equal(messageEnum.Current.MessageId, item.MessageId);
                await receiver.CompleteMessageAsync(item);
            }
        }
        Assert.Equal(0, remaining);

        var peeked = await receiver.PeekMessageAsync();
        Assert.Null(peeked);
    }

    [Fact]
    public async Task AbandonMessages()
    {
        var queueName = await CreateQueueAsync();
        var messageCount = 10;

        var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        var messages = AddAndReturnMessages(batch, messageCount);
        await sender.SendMessagesAsync(batch);

        var receiver = Client.CreateReceiver(queueName);
        var messageEnum = messages.GetEnumerator();
        var remaining = messageCount;
        var receivedMessages = new List<ServiceBusReceivedMessage>();
        while (remaining > 0)
        {
            foreach (var msg in await receiver.ReceiveMessagesAsync(remaining))
            {
                remaining--;
                messageEnum.MoveNext();
                Assert.Equal(messageEnum.Current.MessageId, msg.MessageId);
                receivedMessages.Add(msg);
                Assert.Equal(1, msg.DeliveryCount);
            }
        }
        Assert.Equal(0, remaining);

        foreach (var msg in receivedMessages)
            await receiver.AbandonMessageAsync(msg);

        // After abandon, messages should be available again
        var peekedAfterAbandon = await receiver.PeekMessagesAsync(messageCount);
        Assert.Equal(messageCount, peekedAfterAbandon.Count);
        for (int i = 0; i < peekedAfterAbandon.Count; i++)
            Assert.Equal(messages[i].MessageId, peekedAfterAbandon[i].MessageId);
    }

    [Fact]
    public async Task DeadLetterMessages()
    {
        var queueName = await CreateQueueAsync();
        var messageCount = 10;

        var sender = Client.CreateSender(queueName);
        var messages = GetMessages(messageCount);
        await sender.SendMessagesAsync(messages);

        var receiver = Client.CreateReceiver(queueName);
        var remaining = messageCount;
        var messageEnum = messages.GetEnumerator();

        while (remaining > 0)
        {
            foreach (var item in await receiver.ReceiveMessagesAsync(remaining))
            {
                remaining--;
                messageEnum.MoveNext();
                Assert.Equal(messageEnum.Current.MessageId, item.MessageId);
                await receiver.DeadLetterMessageAsync(item);
            }
        }
        Assert.Equal(0, remaining);

        var peeked = await receiver.PeekMessageAsync();
        Assert.Null(peeked);

        // Read from DLQ
        var dlqPath = $"{queueName}/$deadletterqueue";
        var dlqReceiver = Client.CreateReceiver(dlqPath);
        remaining = messageCount;
        var dlqIdx = 0;
        while (remaining > 0)
        {
            foreach (var item in await dlqReceiver.ReceiveMessagesAsync(remaining))
            {
                remaining--;
                Assert.Equal(messages[dlqIdx].MessageId, item.MessageId);
                dlqIdx++;
                await dlqReceiver.CompleteMessageAsync(item);
            }
        }
        Assert.Equal(0, remaining);
    }

    [Fact]
    public async Task DeadLetterMessagesWithReasonAndDescription()
    {
        var queueName = await CreateQueueAsync();

        var sender = Client.CreateSender(queueName);
        var message = GetMessage();
        await sender.SendMessageAsync(message);

        var receiver = Client.CreateReceiver(queueName);
        var received = await receiver.ReceiveMessageAsync();
        Assert.NotNull(received);
        await receiver.DeadLetterMessageAsync(received, "test-reason", "test-description");

        var dlqPath = $"{queueName}/$deadletterqueue";
        var dlqReceiver = Client.CreateReceiver(dlqPath);
        var dlqMsg = await dlqReceiver.ReceiveMessageAsync();
        Assert.NotNull(dlqMsg);
        Assert.Equal("test-reason", dlqMsg.DeadLetterReason);
        Assert.Equal("test-description", dlqMsg.DeadLetterErrorDescription);
    }

    [Fact]
    public async Task DeferMessages()
    {
        var queueName = await CreateQueueAsync();
        var messageCount = 10;

        var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        var messages = AddAndReturnMessages(batch, messageCount);
        await sender.SendMessagesAsync(batch);

        var receiver = Client.CreateReceiver(queueName);
        var messageEnum = messages.GetEnumerator();
        var sequenceNumbers = new List<long>();
        var remaining = messageCount;

        while (remaining > 0)
        {
            foreach (var item in await receiver.ReceiveMessagesAsync(remaining))
            {
                remaining--;
                messageEnum.MoveNext();
                Assert.Equal(messageEnum.Current.MessageId, item.MessageId);
                sequenceNumbers.Add(item.SequenceNumber);
                await receiver.DeferMessageAsync(item);
            }
        }
        Assert.Equal(0, remaining);

        var deferredMessages = await receiver.ReceiveDeferredMessagesAsync(sequenceNumbers);
        Assert.Equal(messages.Count, deferredMessages.Count);
        for (int i = 0; i < messages.Count; i++)
        {
            Assert.Equal(messages[i].MessageId, deferredMessages[i].MessageId);
            Assert.Equal(messages[i].Body.ToArray(), deferredMessages[i].Body.ToArray());
            Assert.Equal(ServiceBusMessageState.Deferred, deferredMessages[i].State);
        }
    }

    [Fact]
    public async Task CanPeekADeferredMessage()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        var receiver = Client.CreateReceiver(queueName);
        var receivedMsg = await receiver.ReceiveMessageAsync();
        Assert.NotNull(receivedMsg);

        await receiver.DeferMessageAsync(receivedMsg);
        var peekedMsg = await receiver.PeekMessageAsync();
        Assert.NotNull(peekedMsg);
        Assert.Equal(receivedMsg.MessageId, peekedMsg.MessageId);
        Assert.Equal(receivedMsg.SequenceNumber, peekedMsg.SequenceNumber);
        Assert.Equal(ServiceBusMessageState.Deferred, peekedMsg.State);

        var deferredMsg = await receiver.ReceiveDeferredMessageAsync(peekedMsg.SequenceNumber);
        Assert.Equal(peekedMsg.MessageId, deferredMsg.MessageId);
    }

    [Fact]
    public async Task RenewMessageLock()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        var receiver = Client.CreateReceiver(queueName);
        var receivedMessages = (await receiver.ReceiveMessagesAsync(1)).ToArray();
        var receivedMessage = receivedMessages.First();
        var firstLockedUntil = receivedMessage.LockedUntil;

        await Task.Delay(2000);
        await receiver.RenewMessageLockAsync(receivedMessage);
        Assert.True(receivedMessage.LockedUntil > firstLockedUntil);

        await receiver.CompleteMessageAsync(receivedMessage);
        var peeked = await receiver.PeekMessageAsync();
        Assert.Null(peeked);
    }

    [Fact]
    public async Task CanRenewWithSeparateReceiver()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        var receiver1 = Client.CreateReceiver(queueName);
        var message1 = await receiver1.ReceiveMessageAsync();
        Assert.NotNull(message1);
        await receiver1.RenewMessageLockAsync(message1);

        var receiver2 = Client.CreateReceiver(queueName);
        await receiver2.RenewMessageLockAsync(message1);
        await receiver2.CompleteMessageAsync(message1);
    }

    [Fact]
    public async Task ReceiverThrowsWhenUsingSessionEntity()
    {
        var queueName = await CreateQueueAsync(enableSession: true);
        var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage("sessionId"));

        var receiver = Client.CreateReceiver(queueName);
        // Accept either InvalidOperationException (real ASB) or ServiceBusException
        // (emulator returns com.microsoft:session-required at link attach).
        var ex = await Record.ExceptionAsync(() => receiver.ReceiveMessageAsync());
        Assert.NotNull(ex);
        Assert.True(ex is InvalidOperationException || ex is Azure.Messaging.ServiceBus.ServiceBusException,
            $"Expected InvalidOperationException or ServiceBusException, got {ex.GetType().Name}");
    }

    [Fact]
    public async Task ReceiveMessagesWhenQueueEmpty()
    {
        var queueName = await CreateQueueAsync();

        var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        AddAndReturnMessages(batch, 2);
        await sender.SendMessagesAsync(batch);

        var receiver = Client.CreateReceiver(queueName);
        foreach (var msg in await receiver.ReceiveMessagesAsync(2))
            await receiver.CompleteMessageAsync(msg);

        // Now queue is empty - cancellation should be respected
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        var start = DateTime.UtcNow;
        await Assert.ThrowsAsync<TaskCanceledException>(() =>
            receiver.ReceiveMessagesAsync(1, cancellationToken: cts.Token));
        var elapsed = DateTime.UtcNow - start;
        Assert.True(elapsed < TimeSpan.FromSeconds(10), $"Should have cancelled within ~3 seconds, took {elapsed}");
    }

    [Fact]
    public async Task MaxWaitTimeRespected()
    {
        var queueName = await CreateQueueAsync();
        var receiver = Client.CreateReceiver(queueName);

        var start = DateTime.UtcNow;
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        var elapsed = DateTime.UtcNow - start;

        Assert.Null(msg);
        Assert.True(elapsed < TimeSpan.FromSeconds(10), $"MaxWaitTime should have been respected, took {elapsed}");
    }

    [Fact]
    public async Task AbandonMessageModifiesProperties()
    {
        var queueName = await CreateQueueAsync();
        var sender = Client.CreateSender(queueName);
        await sender.SendMessageAsync(GetMessage());

        var receiver = Client.CreateReceiver(queueName);
        var message = await receiver.ReceiveMessageAsync();
        Assert.NotNull(message);

        await receiver.AbandonMessageAsync(message, new Dictionary<string, object> { { "test key", "test value" } });

        var reReceived = await receiver.ReceiveMessageAsync();
        Assert.NotNull(reReceived);
        Assert.Equal("test value", reReceived.ApplicationProperties["test key"]);
    }

    [Fact]
    public async Task ServerBusyRespected()
    {
        var queueName = await CreateQueueAsync();
        var messageCount = 100;

        var sender = Client.CreateSender(queueName);
        using var batch = await sender.CreateMessageBatchAsync();
        AddAndReturnMessages(batch, messageCount);
        await sender.SendMessagesAsync(batch);

        var receiver = Client.CreateReceiver(queueName);
        var remaining = messageCount;
        while (remaining > 0)
        {
            var tasks = new List<Task>();
            foreach (var item in await receiver.ReceiveMessagesAsync(remaining))
            {
                remaining--;
                tasks.Add(receiver.CompleteMessageAsync(item));
            }
            await Task.WhenAll(tasks);
        }
        Assert.Equal(0, remaining);

        var peeked = await receiver.PeekMessageAsync();
        Assert.Null(peeked);
    }
}
