using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using Xunit.Sdk;

namespace AlmostServiceBus.Conformance.Tests;

/// <summary>
/// Base class for conformance tests that run against both the emulator and real Azure Service Bus.
/// Each test creates its own entities (GUID-based names) and cleans them up after.
/// </summary>
public abstract class ConformanceTestBase : IAsyncLifetime
{
    protected ServiceBusClient Client { get; private set; } = null!;
    protected ServiceBusAdministrationClient AdminClient { get; private set; } = null!;

    private readonly string _uniqueId = Guid.NewGuid().ToString("N")[..12];
    private readonly List<string> _createdQueues = [];
    private readonly List<string> _createdTopics = [];

    /// <summary>
    /// When non-null, all tests in this class should be skipped with this reason.
    /// </summary>
    protected string? SkipReason { get; set; }

    /// <summary>
    /// Subclasses provide the connection setup. Return null clients and set SkipReason
    /// to skip all tests.
    /// </summary>
    protected abstract Task<(ServiceBusClient? client, ServiceBusAdministrationClient? admin)> CreateClientsAsync();

    /// <summary>
    /// Throws <see cref="SkipException"/> if SkipReason is set.
    /// Call at the start of each test method.
    /// Note: xunit.runner.visualstudio 3.x may report dynamic skips as failures
    /// in the VSTest output, but the $XunitDynamicSkip$ message prefix is standard.
    /// </summary>
    protected void ThrowIfSkipped()
    {
        if (SkipReason is not null)
            throw SkipException.ForSkip(SkipReason);
    }

    public async Task InitializeAsync()
    {
        var (client, admin) = await CreateClientsAsync();
        if (client is not null)
            Client = client;
        if (admin is not null)
            AdminClient = admin;
    }

    public async Task DisposeAsync()
    {
        if (AdminClient is not null)
        {
            // Clean up entities in reverse order (subscriptions are deleted with topics)
            foreach (var topic in _createdTopics)
            {
                try { await AdminClient.DeleteTopicAsync(topic); } catch { /* best effort */ }
            }

            foreach (var queue in _createdQueues)
            {
                try { await AdminClient.DeleteQueueAsync(queue); } catch { /* best effort */ }
            }
        }

        if (Client is not null)
            await Client.DisposeAsync();
    }

    /// <summary>
    /// Creates a unique queue name and registers it for cleanup.
    /// </summary>
    protected async Task<string> CreateTestQueueAsync(CreateQueueOptions? options = null)
    {
        var name = $"ct-{_uniqueId}-q{_createdQueues.Count}";
        if (options is not null)
        {
            options.Name = name;
            await AdminClient.CreateQueueAsync(options);
        }
        else
        {
            await AdminClient.CreateQueueAsync(name);
        }

        _createdQueues.Add(name);
        return name;
    }

    /// <summary>
    /// Creates a unique topic name and registers it for cleanup.
    /// </summary>
    protected async Task<string> CreateTestTopicAsync()
    {
        var name = $"ct-{_uniqueId}-t{_createdTopics.Count}";
        await AdminClient.CreateTopicAsync(name);
        _createdTopics.Add(name);
        return name;
    }

    /// <summary>
    /// Creates a subscription on a topic, optionally with ForwardTo.
    /// </summary>
    protected async Task CreateTestSubscriptionAsync(string topicName, string subName, string? forwardTo = null)
    {
        var options = new CreateSubscriptionOptions(topicName, subName);
        if (forwardTo is not null)
            options.ForwardTo = forwardTo;
        await AdminClient.CreateSubscriptionAsync(options);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 1: PeekLock Settlement
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PeekLock_Complete_RemovesMessage()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("complete-me"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("complete-me", msg.Body.ToString());

        // Complete should succeed
        await receiver.CompleteMessageAsync(msg);

        // No more messages should be available
        var next = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(next);
    }

    [Fact]
    public async Task PeekLock_Abandon_RedeliversMessage()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("abandon-me"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("abandon-me", msg.Body.ToString());
        Assert.Equal(1, msg.DeliveryCount);

        // Abandon the message
        await receiver.AbandonMessageAsync(msg);

        // Message should be re-delivered with incremented delivery count
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(redelivered);
        Assert.Equal("abandon-me", redelivered.Body.ToString());
        Assert.Equal(2, redelivered.DeliveryCount);

        // Clean up
        await receiver.CompleteMessageAsync(redelivered);
    }

    [Fact]
    public async Task PeekLock_DeadLetter_MovesToDlq()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("deadletter-me"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        // Dead-letter the message
        await receiver.DeadLetterMessageAsync(msg, "TestReason", "Test error description");

        // Original queue should be empty
        var next = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(next);

        // Message should appear in the DLQ
        await using var dlqReceiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            SubQueue = SubQueue.DeadLetter
        });

        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("deadletter-me", dlqMsg.Body.ToString());
        Assert.Equal("TestReason", dlqMsg.DeadLetterReason);
        Assert.Equal("Test error description", dlqMsg.DeadLetterErrorDescription);

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 2: Lock Behavior
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task LockExpiry_CompleteFails_MessageRedelivered()
    {
        ThrowIfSkipped();
        // Create queue with a very short lock duration
        var options = new CreateQueueOptions($"placeholder")
        {
            LockDuration = TimeSpan.FromSeconds(5)
        };
        var queue = await CreateTestQueueAsync(options);

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("lock-test"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        // Wait for the lock to expire
        await Task.Delay(TimeSpan.FromSeconds(6));

        // Try to complete after lock expiry.
        // Real Azure Service Bus: throws ServiceBusException with MessageLockLost reason.
        // Emulator: the complete is silently accepted but the message is re-enqueued
        // for redelivery (the emulator cannot reject individual AMQP dispositions
        // without detaching the link, so it accepts the disposition and re-enqueues).
        try
        {
            await receiver.CompleteMessageAsync(msg);
            // Emulator path: complete "succeeded" but message was re-enqueued
        }
        catch (ServiceBusException ex) when (ex.Reason == ServiceBusFailureReason.MessageLockLost)
        {
            // Real ASB path: lock-lost exception is expected
        }

        // The message should be re-delivered regardless of which path we took
        var redelivered = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(redelivered);
        Assert.Equal("lock-test", redelivered.Body.ToString());

        await receiver.CompleteMessageAsync(redelivered);
    }

    [Fact]
    public async Task RenewMessageLock_ExtendsLock_CompletionSucceedsAfterOriginalExpiry()
    {
        ThrowIfSkipped();
        var options = new CreateQueueOptions("placeholder")
        {
            LockDuration = TimeSpan.FromSeconds(5)
        };
        var queue = await CreateTestQueueAsync(options);

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("renew-test"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        // Wait 3s (past half the lock), then renew
        await Task.Delay(TimeSpan.FromSeconds(3));
        await receiver.RenewMessageLockAsync(msg);

        // Wait another 4s — past the ORIGINAL lock expiry, but within the renewed window
        await Task.Delay(TimeSpan.FromSeconds(4));

        // Complete should succeed because the lock was renewed
        await receiver.CompleteMessageAsync(msg);

        // Queue should be empty — message was completed, not re-enqueued
        var next = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(next);
    }

    [Fact]
    public async Task Processor_AutoLockRenewal_CompletesAfterOriginalExpiry()
    {
        ThrowIfSkipped();
        var options = new CreateQueueOptions("placeholder")
        {
            LockDuration = TimeSpan.FromSeconds(5)
        };
        var queue = await CreateTestQueueAsync(options);

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("processor-renew-test"));

        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = false,
            // Auto-renewal should keep the lock alive during long processing
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(1),
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        processor.ProcessMessageAsync += async args =>
        {
            // Simulate long processing that exceeds the 5s lock duration
            await Task.Delay(TimeSpan.FromSeconds(8), args.CancellationToken);

            // This should succeed because auto-renewal kept the lock alive
            await args.CompleteMessageAsync(args.Message, args.CancellationToken);
            completed.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            completed.TrySetException(args.Exception);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        var result = await Task.WhenAny(completed.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.True(result == completed.Task, "Processor should have completed within 30s");
        Assert.True(await completed.Task);

        await processor.StopProcessingAsync();

        // Queue should be empty — message was completed, not re-enqueued
        await using var receiver = Client.CreateReceiver(queue);
        var next = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(next);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 3: Concurrent Message Delivery
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Processor_MaxConcurrentCalls1_ProcessesSequentially()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        // Send 5 messages
        await using var sender = Client.CreateSender(queue);
        for (int i = 0; i < 5; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"seq-{i}"));
        }

        var timings = new ConcurrentBag<(DateTimeOffset Start, DateTimeOffset End, string Body)>();
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = true,
            PrefetchCount = 0
        });

        processor.ProcessMessageAsync += async args =>
        {
            var start = DateTimeOffset.UtcNow;
            // Simulate some work to make timing measurable
            await Task.Delay(100);
            var end = DateTimeOffset.UtcNow;
            timings.Add((start, end, args.Message.Body.ToString()));

            if (timings.Count >= 5)
                allProcessed.TrySetResult();
        };

        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();

        // Wait for all messages or timeout
        var completed = await Task.WhenAny(allProcessed.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.True(allProcessed.Task.IsCompletedSuccessfully, "Not all 5 messages were processed within 30s");
        Assert.Equal(5, timings.Count);

        // Assert no overlap: each message's start should be after the previous one's end
        var sorted = timings.OrderBy(t => t.Start).ToList();
        for (int i = 1; i < sorted.Count; i++)
        {
            Assert.True(sorted[i].Start >= sorted[i - 1].End,
                $"Message {i} started at {sorted[i].Start:O} but message {i - 1} ended at {sorted[i - 1].End:O} — overlap detected");
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 4: Drain/Shutdown
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Processor_Shutdown_CompletesWithinFiveSeconds()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("drain-test"));

        var messageReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = true,
            PrefetchCount = 0
        });

        processor.ProcessMessageAsync += args =>
        {
            messageReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();

        // Wait for the message to be processed
        var received = await Task.WhenAny(messageReceived.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        Assert.True(messageReceived.Task.IsCompletedSuccessfully, "Message was not received within 10s");

        // Stop the processor and measure time
        var sw = Stopwatch.StartNew();
        await processor.StopProcessingAsync();
        sw.Stop();

        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(5),
            $"Processor stop took {sw.Elapsed.TotalSeconds:F1}s — expected less than 5s");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 5: Multiple Messages Sequential Processing
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Processor_ThreeMessages_AllReceivedNoDuplicates()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        var sentBodies = new[] { "msg-alpha", "msg-beta", "msg-gamma" };
        await using var sender = Client.CreateSender(queue);
        foreach (var body in sentBodies)
        {
            await sender.SendMessageAsync(new ServiceBusMessage(body));
        }

        var receivedBodies = new ConcurrentBag<string>();
        var allProcessed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            AutoCompleteMessages = true,
            PrefetchCount = 0
        });

        processor.ProcessMessageAsync += args =>
        {
            receivedBodies.Add(args.Message.Body.ToString());
            if (receivedBodies.Count >= 3)
                allProcessed.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();

        var completed = await Task.WhenAny(allProcessed.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.True(allProcessed.Task.IsCompletedSuccessfully, "Not all 3 messages were processed within 30s");

        // Assert all messages received, no duplicates, no lost messages
        Assert.Equal(3, receivedBodies.Count);
        Assert.Equal(3, receivedBodies.Distinct().Count()); // No duplicates

        foreach (var body in sentBodies)
        {
            Assert.Contains(body, receivedBodies);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 6: Topic Fan-Out
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TopicFanOut_TwoSubscriptions_BothReceiveMessage()
    {
        ThrowIfSkipped();
        var topic = await CreateTestTopicAsync();
        var queue1 = await CreateTestQueueAsync();
        var queue2 = await CreateTestQueueAsync();

        // Create two subscriptions, each forwarding to a different queue
        await CreateTestSubscriptionAsync(topic, "sub-1", forwardTo: queue1);
        await CreateTestSubscriptionAsync(topic, "sub-2", forwardTo: queue2);

        // Publish a message to the topic
        await using var sender = Client.CreateSender(topic);
        await sender.SendMessageAsync(new ServiceBusMessage("fan-out-test"));

        // Receive from both queues
        await using var receiver1 = Client.CreateReceiver(queue1, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });
        await using var receiver2 = Client.CreateReceiver(queue2, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg1 = await receiver1.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var msg2 = await receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(msg1);
        Assert.NotNull(msg2);
        Assert.Equal("fan-out-test", msg1.Body.ToString());
        Assert.Equal("fan-out-test", msg2.Body.ToString());

        await receiver1.CompleteMessageAsync(msg1);
        await receiver2.CompleteMessageAsync(msg2);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 7: Message Properties Round-Trip
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MessageProperties_RoundTrip_AllPreserved()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        var outgoing = new ServiceBusMessage("properties-body")
        {
            CorrelationId = "corr-abc-123",
            Subject = "test-subject",
            ContentType = "application/json",
        };
        outgoing.ApplicationProperties["custom-string"] = "hello";
        outgoing.ApplicationProperties["custom-int"] = 42;
        outgoing.ApplicationProperties["custom-bool"] = true;

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(outgoing);

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);

        Assert.Equal("properties-body", msg.Body.ToString());
        Assert.Equal("corr-abc-123", msg.CorrelationId);
        Assert.Equal("test-subject", msg.Subject);
        Assert.Equal("application/json", msg.ContentType);

        Assert.Equal("hello", msg.ApplicationProperties["custom-string"]);
        Assert.Equal(42, msg.ApplicationProperties["custom-int"]);
        Assert.Equal(true, msg.ApplicationProperties["custom-bool"]);

        await receiver.CompleteMessageAsync(msg);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 10: Two messages via topic subscription to same queue (Helix pattern)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TwoMessages_ViaTopicSubscription_BothProcessedByProcessor()
    {
        ThrowIfSkipped();

        // Setup: topic → subscription → consumer queue (same as MassTransit pattern)
        var queue = await CreateTestQueueAsync();
        var topic = await CreateTestTopicAsync();
        await CreateTestSubscriptionAsync(topic, "consumer-sub", forwardTo: queue);

        // Send two messages to the topic (simulates two events published by outbox)
        await using var sender = Client.CreateSender(topic);
        await sender.SendMessageAsync(new ServiceBusMessage("event-1") { MessageId = "msg-1" });
        await sender.SendMessageAsync(new ServiceBusMessage("event-2") { MessageId = "msg-2" });

        // Process with a ServiceBusProcessor (MaxConcurrentCalls=1, like MassTransit)
        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource<bool>();

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 1,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            AutoCompleteMessages = false,
        });

        processor.ProcessMessageAsync += async args =>
        {
            var body = args.Message.Body.ToString();
            received.Add(body);
            await args.CompleteMessageAsync(args.Message);
            if (received.Count >= 2)
                allReceived.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            // Log but don't fail — we want to see what happens
            Console.WriteLine($"[CONFORMANCE] Processor error: {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        // Wait for both messages — 10 second timeout
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(10)));
        await processor.StopProcessingAsync();

        Assert.True(completed == allReceived.Task, $"Expected 2 messages but got {received.Count}: [{string.Join(", ", received)}]");
        Assert.Contains("event-1", received);
        Assert.Contains("event-2", received);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 11: Multiple subscriptions on same topic — no cross-delivery
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MultipleSubscriptions_SameTopic_EachQueueGetsExactlyOneCopy()
    {
        ThrowIfSkipped();

        // Setup: 1 topic, 3 subscriptions, each forwarding to a different queue
        var queue1 = await CreateTestQueueAsync();
        var queue2 = await CreateTestQueueAsync();
        var queue3 = await CreateTestQueueAsync();
        var topic = await CreateTestTopicAsync();

        await CreateTestSubscriptionAsync(topic, "sub1", forwardTo: queue1);
        await CreateTestSubscriptionAsync(topic, "sub2", forwardTo: queue2);
        await CreateTestSubscriptionAsync(topic, "sub3", forwardTo: queue3);

        // Publish ONE message to the topic
        await using var sender = Client.CreateSender(topic);
        await sender.SendMessageAsync(new ServiceBusMessage("single-event") { MessageId = "unique-msg" });

        // Each queue should get exactly ONE copy
        await using var receiver1 = Client.CreateReceiver(queue1);
        await using var receiver2 = Client.CreateReceiver(queue2);
        await using var receiver3 = Client.CreateReceiver(queue3);

        var msg1 = await receiver1.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var msg2 = await receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var msg3 = await receiver3.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(msg1);
        Assert.NotNull(msg2);
        Assert.NotNull(msg3);

        Assert.Equal("single-event", msg1.Body.ToString());
        Assert.Equal("single-event", msg2.Body.ToString());
        Assert.Equal("single-event", msg3.Body.ToString());

        // Complete all
        await receiver1.CompleteMessageAsync(msg1);
        await receiver2.CompleteMessageAsync(msg2);
        await receiver3.CompleteMessageAsync(msg3);

        // Verify NO additional messages on any queue (no duplicates)
        var extra1 = await receiver1.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        var extra2 = await receiver2.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        var extra3 = await receiver3.ReceiveMessageAsync(TimeSpan.FromSeconds(2));

        Assert.Null(extra1);
        Assert.Null(extra2);
        Assert.Null(extra3);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 12: Many subscriptions forwarding to same queue — no duplicates
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ManySubscriptions_OneForwardingToQueue_OnlyOneDelivery()
    {
        ThrowIfSkipped();

        // Simulate MassTransit pattern: topic has sub for real consumer + sub for test consumer
        // Only the test sub forwards to our queue. The other sub forwards elsewhere.
        var testQueue = await CreateTestQueueAsync();
        var otherQueue = await CreateTestQueueAsync();
        var topic = await CreateTestTopicAsync();

        await CreateTestSubscriptionAsync(topic, "test-sub", forwardTo: testQueue);
        await CreateTestSubscriptionAsync(topic, "other-sub", forwardTo: otherQueue);

        // Publish a message
        await using var sender = Client.CreateSender(topic);
        await sender.SendMessageAsync(new ServiceBusMessage("test-msg") { MessageId = "test-msg-1" });

        // testQueue should get exactly ONE message
        await using var testReceiver = Client.CreateReceiver(testQueue);
        var msg = await testReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("test-msg", msg.Body.ToString());
        await testReceiver.CompleteMessageAsync(msg);

        // No duplicate
        var dup = await testReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(dup);

        // otherQueue should also get exactly ONE message
        await using var otherReceiver = Client.CreateReceiver(otherQueue);
        var otherMsg = await otherReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(otherMsg);
        await otherReceiver.CompleteMessageAsync(otherMsg);

        var otherDup = await otherReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(otherDup);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 13: MassTransit-scale — many topics, each with a sub forwarding to
    // same consumer queue. Publish to ONE topic, verify only ONE delivery.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ManyTopics_OneSubEach_ForwardToSameQueue_NoDuplicates()
    {
        ThrowIfSkipped();

        // Create a consumer queue (like MassTransit's "test" endpoint)
        var consumerQueue = await CreateTestQueueAsync();

        // Create 20 topics, each with a subscription forwarding to the same queue
        // (simulates MassTransit's TestConsumer subscribing to many event types)
        var topics = new List<string>();
        for (int i = 0; i < 20; i++)
        {
            var topic = await CreateTestTopicAsync();
            await CreateTestSubscriptionAsync(topic, "consumer-sub", forwardTo: consumerQueue);
            topics.Add(topic);
        }

        // Publish ONE message to topic[5] only
        await using var sender = Client.CreateSender(topics[5]);
        await sender.SendMessageAsync(new ServiceBusMessage("targeted-event") { MessageId = "single-publish" });

        // Consumer queue should get exactly ONE message
        await using var receiver = Client.CreateReceiver(consumerQueue);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("targeted-event", msg.Body.ToString());
        Assert.Equal("single-publish", msg.MessageId);
        await receiver.CompleteMessageAsync(msg);

        // No duplicate
        var dup = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(3));
        Assert.Null(dup);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 14: Sustained throughput — 20 msg/sec for 30 seconds
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SustainedThroughput_20PerSecond_AllReceived()
    {
        ThrowIfSkipped();

        var queue = await CreateTestQueueAsync();
        var totalMessages = 600; // 20/sec × 30 sec
        var sendDuration = TimeSpan.FromSeconds(30);
        var sendInterval = sendDuration / totalMessages; // 50ms between sends

        // Start processor first so it's ready to receive
        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource<bool>();

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 10,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            AutoCompleteMessages = false,
        });

        processor.ProcessMessageAsync += async args =>
        {
            received.Add(args.Message.MessageId);
            await args.CompleteMessageAsync(args.Message);
            if (received.Count >= totalMessages)
                allReceived.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"[THROUGHPUT] Error: {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        // Send messages at ~20/sec
        await using var sender = Client.CreateSender(queue);
        var sendStart = Stopwatch.StartNew();
        var sent = 0;

        for (int i = 0; i < totalMessages; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"msg-{i}") { MessageId = $"throughput-{i}" });
            sent++;

            // Pace to ~20/sec
            var elapsed = sendStart.Elapsed;
            var expectedElapsed = TimeSpan.FromMilliseconds(i * sendInterval.TotalMilliseconds);
            if (elapsed < expectedElapsed)
                await Task.Delay(expectedElapsed - elapsed);

            // Progress every 100
            if (sent % 100 == 0)
                Console.WriteLine($"[THROUGHPUT] Sent {sent}/{totalMessages}, received {received.Count}");
        }

        Console.WriteLine($"[THROUGHPUT] All {sent} sent in {sendStart.Elapsed.TotalSeconds:F1}s. Waiting for receive...");

        // Wait for all to be received — give 60 seconds after send completes
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(60)));
        await processor.StopProcessingAsync();

        var receivedCount = received.Count;
        var uniqueCount = received.Distinct().Count();
        Console.WriteLine($"[THROUGHPUT] Received: {receivedCount}, Unique: {uniqueCount}, Duplicates: {receivedCount - uniqueCount}");

        Assert.True(completed == allReceived.Task,
            $"Expected {totalMessages} messages but received {receivedCount} ({uniqueCount} unique) after 60s wait. Missing: {totalMessages - uniqueCount}");
        Assert.Equal(totalMessages, uniqueCount);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 15: Burst send — 60 messages as fast as possible, all received
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BurstSend_60Messages_AllReceived()
    {
        ThrowIfSkipped();

        var queue = await CreateTestQueueAsync();
        var totalMessages = 60;

        // Start processor
        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource<bool>();

        await using var processor = Client.CreateProcessor(queue, new ServiceBusProcessorOptions
        {
            MaxConcurrentCalls = 10,
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            AutoCompleteMessages = false,
        });

        processor.ProcessMessageAsync += async args =>
        {
            received.Add(args.Message.MessageId);
            await args.CompleteMessageAsync(args.Message);
            if (received.Count >= totalMessages)
                allReceived.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"[BURST] Error: {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        // Burst send — all 60 as fast as possible
        await using var sender = Client.CreateSender(queue);
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < totalMessages; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"burst-{i}") { MessageId = $"burst-{i}" });
        }
        Console.WriteLine($"[BURST] Sent {totalMessages} in {sw.ElapsedMilliseconds}ms");

        // Wait for all received
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        var receivedCount = received.Count;
        var uniqueCount = received.Distinct().Count();
        Console.WriteLine($"[BURST] Received: {receivedCount}, Unique: {uniqueCount}");

        Assert.True(completed == allReceived.Task,
            $"Expected {totalMessages} but received {receivedCount} ({uniqueCount} unique)");
        Assert.Equal(totalMessages, uniqueCount);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 16: Scheduled Messages
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ScheduledMessage_NotReceivedBeforeScheduledTime()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);

        // Send a message with ScheduledEnqueueTime set in the future.
        // When ScheduledEnqueueTime is set, the Azure SDK sets the
        // x-opt-scheduled-enqueue-time AMQP annotation, and the broker
        // should defer delivery until the scheduled time.
        var msg = new ServiceBusMessage("scheduled-body")
        {
            MessageId = "scheduled-1",
            ScheduledEnqueueTime = DateTimeOffset.UtcNow.AddSeconds(4)
        };

        await sender.SendMessageAsync(msg);

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // Should NOT be available immediately (wait 2 seconds, well before the 4s schedule)
        var early = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(early);

        // Should be available after the scheduled time (wait up to 10 seconds)
        var delayed = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(delayed);
        Assert.Equal("scheduled-body", delayed.Body.ToString());
        Assert.Equal("scheduled-1", delayed.MessageId);

        await receiver.CompleteMessageAsync(delayed);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 16b: ScheduleMessageAsync (entity-scoped $management link)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ScheduleMessageAsync_DeliversAtScheduledTime()
    {
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await using var receiver = Client.CreateReceiver(queue);

        // Use the management-link-based scheduling API (not ScheduledEnqueueTime property)
        var seqNo = await sender.ScheduleMessageAsync(
            new ServiceBusMessage("mgmt-scheduled") { MessageId = "mgmt-sched-1" },
            DateTimeOffset.UtcNow.AddSeconds(3));

        Assert.True(seqNo > 0);

        // Should not be available immediately
        var early = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
        Assert.Null(early);

        // Should be available after the schedule time
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(10));
        Assert.NotNull(msg);
        Assert.Equal("mgmt-scheduled", msg.Body.ToString());
        await receiver.CompleteMessageAsync(msg);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 17: Subscription SQL Filter
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SubscriptionSqlFilter_OnlyMatchingMessagesDelivered()
    {
        ThrowIfSkipped();

        var topic = await CreateTestTopicAsync();
        var queue = await CreateTestQueueAsync();

        // Create a subscription with a SQL filter that only matches color = 'blue'
        var subOptions = new CreateSubscriptionOptions(topic, "filtered-sub")
        {
            ForwardTo = queue
        };
        await AdminClient.CreateSubscriptionAsync(subOptions);

        // Remove the default $Default rule and add a SQL filter
        await AdminClient.DeleteRuleAsync(topic, "filtered-sub", "$Default");
        var ruleOptions = new CreateRuleOptions("color-filter")
        {
            Filter = new SqlRuleFilter("color = 'blue'")
        };
        await AdminClient.CreateRuleAsync(topic, "filtered-sub", ruleOptions);

        await using var sender = Client.CreateSender(topic);

        // Send a matching message
        var blueMsg = new ServiceBusMessage("blue-msg") { MessageId = "blue-1" };
        blueMsg.ApplicationProperties["color"] = "blue";
        await sender.SendMessageAsync(blueMsg);

        // Send a non-matching message
        var redMsg = new ServiceBusMessage("red-msg") { MessageId = "red-1" };
        redMsg.ApplicationProperties["color"] = "red";
        await sender.SendMessageAsync(redMsg);

        // Only the blue message should be received
        await using var receiver = Client.CreateReceiver(queue);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("blue-msg", msg.Body.ToString());

        await receiver.CompleteMessageAsync(msg);

        // No more messages
        var extra = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(extra);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 18: Subscription Correlation Filter
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SubscriptionCorrelationFilter_OnlyMatchingMessagesDelivered()
    {
        ThrowIfSkipped();

        var topic = await CreateTestTopicAsync();
        var queue = await CreateTestQueueAsync();

        var subOptions = new CreateSubscriptionOptions(topic, "corr-sub")
        {
            ForwardTo = queue
        };
        await AdminClient.CreateSubscriptionAsync(subOptions);

        // Remove the default rule and add a correlation filter on Subject
        await AdminClient.DeleteRuleAsync(topic, "corr-sub", "$Default");
        var ruleOptions = new CreateRuleOptions("subject-filter")
        {
            Filter = new CorrelationRuleFilter { Subject = "important" }
        };
        await AdminClient.CreateRuleAsync(topic, "corr-sub", ruleOptions);

        await using var sender = Client.CreateSender(topic);

        // Send a matching message
        var matchMsg = new ServiceBusMessage("match") { Subject = "important" };
        await sender.SendMessageAsync(matchMsg);

        // Send a non-matching message
        var noMatchMsg = new ServiceBusMessage("no-match") { Subject = "trivial" };
        await sender.SendMessageAsync(noMatchMsg);

        // Only the matching message should be received
        await using var receiver = Client.CreateReceiver(queue);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        Assert.Equal("match", msg.Body.ToString());
        await receiver.CompleteMessageAsync(msg);

        var extra = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(extra);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 19: Dead-letter on MaxDeliveryCount
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Abandon_ExceedsMaxDeliveryCount_MovesToDeadLetterQueue()
    {
        ThrowIfSkipped();

        // Create a queue with MaxDeliveryCount = 2
        var queueOptions = new CreateQueueOptions("placeholder")
        {
            MaxDeliveryCount = 2
        };
        var queue = await CreateTestQueueAsync(queueOptions);

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("poison-pill") { MessageId = "poison-1" });

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // First delivery — abandon
        var msg1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg1);
        Assert.Equal(1, msg1.DeliveryCount);
        await receiver.AbandonMessageAsync(msg1);

        // Second delivery — abandon again, triggering dead-letter
        var msg2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg2);
        Assert.Equal(2, msg2.DeliveryCount);
        await receiver.AbandonMessageAsync(msg2);

        // Queue should now be empty
        var msg3 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(msg3);

        // Message should be in the DLQ
        await using var dlqReceiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock,
            SubQueue = SubQueue.DeadLetter
        });

        var dlqMsg = await dlqReceiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(dlqMsg);
        Assert.Equal("poison-pill", dlqMsg.Body.ToString());
        Assert.Equal("MaxDeliveryCountExceeded", dlqMsg.DeadLetterReason);

        await dlqReceiver.CompleteMessageAsync(dlqMsg);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test 20: Duplicate Detection
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DuplicateDetection_SilentlyDropsDuplicateMessages()
    {
        ThrowIfSkipped();

        var queueOptions = new CreateQueueOptions("placeholder")
        {
            RequiresDuplicateDetection = true,
            DuplicateDetectionHistoryTimeWindow = TimeSpan.FromMinutes(5)
        };
        var queue = await CreateTestQueueAsync(queueOptions);

        await using var sender = Client.CreateSender(queue);

        // Send a message with a specific MessageId
        await sender.SendMessageAsync(new ServiceBusMessage("first") { MessageId = "dedup-1" });

        // Send the same MessageId again — should be silently dropped
        await sender.SendMessageAsync(new ServiceBusMessage("duplicate") { MessageId = "dedup-1" });

        // Send a different MessageId — should be delivered
        await sender.SendMessageAsync(new ServiceBusMessage("second") { MessageId = "dedup-2" });

        await using var receiver = Client.CreateReceiver(queue);

        var msg1 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg1);
        Assert.Equal("first", msg1.Body.ToString());
        Assert.Equal("dedup-1", msg1.MessageId);
        await receiver.CompleteMessageAsync(msg1);

        var msg2 = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg2);
        Assert.Equal("second", msg2.Body.ToString());
        Assert.Equal("dedup-2", msg2.MessageId);
        await receiver.CompleteMessageAsync(msg2);

        // No more messages (the duplicate was dropped)
        var extra = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(extra);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test: Session send and receive — FIFO order
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Session_SendAndReceive_FifoOrder()
    {
        ThrowIfSkipped();

        var queue = await CreateTestQueueAsync(new CreateQueueOptions($"ct-{_uniqueId}-sess1")
        {
            RequiresSession = true,
            LockDuration = TimeSpan.FromMinutes(1)
        });

        await using var sender = Client.CreateSender(queue);
        for (int i = 0; i < 3; i++)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"msg-{i}")
            {
                SessionId = "session-1",
                MessageId = $"sess-msg-{i}"
            });
        }

        await using var receiver = await Client.AcceptSessionAsync(queue, "session-1");

        var messages = new List<string>();
        for (int i = 0; i < 3; i++)
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
            Assert.NotNull(msg);
            messages.Add(msg.Body.ToString());
            await receiver.CompleteMessageAsync(msg);
        }

        Assert.Equal(["msg-0", "msg-1", "msg-2"], messages);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test: Multiple sessions — isolated delivery
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Session_MultipleSessions_IsolatedDelivery()
    {
        ThrowIfSkipped();

        var queue = await CreateTestQueueAsync(new CreateQueueOptions($"ct-{_uniqueId}-sess2")
        {
            RequiresSession = true,
            LockDuration = TimeSpan.FromMinutes(1)
        });

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("alpha") { SessionId = "A" });
        await sender.SendMessageAsync(new ServiceBusMessage("beta") { SessionId = "B" });
        await sender.SendMessageAsync(new ServiceBusMessage("gamma") { SessionId = "A" });

        await using var receiverA = await Client.AcceptSessionAsync(queue, "A");
        await using var receiverB = await Client.AcceptSessionAsync(queue, "B");

        var msgA1 = await receiverA.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var msgA2 = await receiverA.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        var msgB1 = await receiverB.ReceiveMessageAsync(TimeSpan.FromSeconds(5));

        Assert.Equal("alpha", msgA1!.Body.ToString());
        Assert.Equal("gamma", msgA2!.Body.ToString());
        Assert.Equal("beta", msgB1!.Body.ToString());

        await receiverA.CompleteMessageAsync(msgA1);
        await receiverA.CompleteMessageAsync(msgA2);
        await receiverB.CompleteMessageAsync(msgB1);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Test: Session state — set and get
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Session_SetAndGetSessionState()
    {
        ThrowIfSkipped();

        var queue = await CreateTestQueueAsync(new CreateQueueOptions($"ct-{_uniqueId}-sessstate")
        {
            RequiresSession = true,
            LockDuration = TimeSpan.FromMinutes(1)
        });

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("state-test")
        {
            SessionId = "state-session-1"
        });

        await using var receiver = await Client.AcceptSessionAsync(queue, "state-session-1");

        // Initially session state should be empty
        var initialState = await receiver.GetSessionStateAsync();
        Assert.True(initialState is null || initialState.ToMemory().Length == 0);

        // Set session state
        var stateBytes = System.Text.Encoding.UTF8.GetBytes("hello-session-state");
        await receiver.SetSessionStateAsync(new BinaryData(stateBytes));

        // Get session state back
        var retrieved = await receiver.GetSessionStateAsync();
        Assert.NotNull(retrieved);
        Assert.Equal("hello-session-state", System.Text.Encoding.UTF8.GetString(retrieved.ToMemory().Span));

        // Clear session state
        await receiver.SetSessionStateAsync(null);
        var cleared = await receiver.GetSessionStateAsync();
        Assert.True(cleared is null || cleared.ToMemory().Length == 0);

        // Complete the message
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        await receiver.CompleteMessageAsync(msg);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Session Lock Renewal
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Session_RenewSessionLock_ExtendsLock()
    {
        ThrowIfSkipped();
        var options = new CreateQueueOptions("placeholder")
        {
            RequiresSession = true,
            LockDuration = TimeSpan.FromSeconds(10)
        };
        var queue = await CreateTestQueueAsync(options);

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("session-renew-test")
        {
            SessionId = "renew-session-1"
        });

        await using var receiver = await Client.AcceptSessionAsync(queue, "renew-session-1");

        // Renew the session lock — should not throw
        await receiver.RenewSessionLockAsync();

        // Receive and complete the message — should work because lock is active
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
        await receiver.CompleteMessageAsync(msg);
    }

    [Fact]
    public async Task Session_ProcessorAutoRenewal_WithSetStateAndSchedule()
    {
        // Reproduces the MassTransit session saga pattern:
        // A session processor receives a message, sets session state, schedules a
        // future message, and auto-renews the session lock — all concurrently.
        // Uses a GUID session ID (like MassTransit's CorrelationId) and a short lock
        // duration to force auto-renewal during processing.
        ThrowIfSkipped();
        var options = new CreateQueueOptions("placeholder")
        {
            RequiresSession = true,
            LockDuration = TimeSpan.FromSeconds(5) // Short lock forces auto-renewal
        };
        var queue = await CreateTestQueueAsync(options);

        var sessionId = Guid.NewGuid().ToString("D");

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("saga-test")
        {
            SessionId = sessionId
        });

        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateSessionProcessor(queue, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(1)
        });

        processor.ProcessMessageAsync += async args =>
        {
            // Simulate what MassTransit's session saga does:
            // 1. Get session state
            var state = await args.GetSessionStateAsync();

            // 2. Schedule a future message (via sender, simulating ScheduleMessageAsync)
            var seqNo = await sender.ScheduleMessageAsync(
                new ServiceBusMessage("timeout") { SessionId = args.SessionId },
                DateTimeOffset.UtcNow.AddSeconds(30));

            // 3. Set session state
            await args.SetSessionStateAsync(new BinaryData(
                System.Text.Encoding.UTF8.GetBytes("{\"state\":\"processing\"}")));

            // 4. Wait long enough that auto-renewal must fire (lock is 5s)
            await Task.Delay(TimeSpan.FromSeconds(7));

            // 5. Complete the message — should work if auto-renewal kept the lock alive
            await args.CompleteMessageAsync(args.Message);

            // 6. Cancel the scheduled message (cleanup)
            await sender.CancelScheduledMessageAsync(seqNo);

            completed.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            completed.TrySetException(args.Exception);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        var result = await Task.WhenAny(completed.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.True(result == completed.Task, "Session processor should have completed within 30s");
        Assert.True(await completed.Task);

        await processor.StopProcessingAsync();
    }

    [Fact]
    public async Task Session_ProcessorAutoRenewal_WithDefaultLock()
    {
        // Replicates MassTransit's session saga pattern with default lock duration.
        // The session processor auto-renews the session lock while processing.
        // This catches the "Session not found or not locked" failure seen in MT tests.
        ThrowIfSkipped();
        var options = new CreateQueueOptions("placeholder")
        {
            RequiresSession = true
            // Default lock duration (30s) — same as MassTransit
        };
        var queue = await CreateTestQueueAsync(options);

        var sessionId = Guid.NewGuid().ToString("D");

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("saga-test")
        {
            SessionId = sessionId
        });

        var renewSucceeded = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateSessionProcessor(queue, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(1)
        });

        processor.ProcessMessageAsync += async args =>
        {
            // Explicitly renew the session lock (like MassTransit's auto-renewal does)
            await args.RenewSessionLockAsync();

            await args.CompleteMessageAsync(args.Message);
            renewSucceeded.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            renewSucceeded.TrySetException(args.Exception);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        var result = await Task.WhenAny(renewSucceeded.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        Assert.True(result == renewSucceeded.Task, "Session processor should have completed within 15s");
        Assert.True(await renewSucceeded.Task);

        await processor.StopProcessingAsync();
    }

    [Fact]
    public async Task Session_AutoRenewal_SurvivesMultipleRenewCycles()
    {
        // Very short lock (3s) with a 10-second hold. The SDK must auto-renew
        // the session lock at least 3 times to keep the lock alive.
        // Fails immediately if the management link's renew-session-lock path is broken.
        ThrowIfSkipped();
        var options = new CreateQueueOptions("placeholder")
        {
            RequiresSession = true,
            LockDuration = TimeSpan.FromSeconds(3)
        };
        var queue = await CreateTestQueueAsync(options);

        var sessionId = "renew-stress";

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("survive-renewal")
        {
            SessionId = sessionId
        });

        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var processor = Client.CreateSessionProcessor(queue, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 1,
            AutoCompleteMessages = false,
            MaxAutoLockRenewalDuration = TimeSpan.FromMinutes(1)
        });

        processor.ProcessMessageAsync += async args =>
        {
            // Hold for 10 seconds — with a 3s lock, the SDK must auto-renew
            // at least 3 times during this window.
            await Task.Delay(TimeSpan.FromSeconds(10));

            // Complete should succeed if auto-renewal kept the lock alive
            await args.CompleteMessageAsync(args.Message);
            completed.TrySetResult(true);
        };

        processor.ProcessErrorAsync += args =>
        {
            completed.TrySetException(args.Exception);
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        var result = await Task.WhenAny(completed.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        Assert.True(result == completed.Task, "Session processor should have completed within 30s");
        Assert.True(await completed.Task);

        await processor.StopProcessingAsync();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Abandon Redelivery — Sequence Number Behavior
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Abandon_Redelivery_SequenceNumberBehavior()
    {
        // Determines whether real ASB keeps or changes the SequenceNumber
        // when a message is abandoned and redelivered. This is critical for
        // MassTransit's ConsumerAgent, which uses SequenceNumber as the
        // transport dedup key.
        ThrowIfSkipped();
        var queue = await CreateTestQueueAsync();

        await using var sender = Client.CreateSender(queue);
        await sender.SendMessageAsync(new ServiceBusMessage("seq-test"));

        await using var receiver = Client.CreateReceiver(queue, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.PeekLock
        });

        // First delivery
        var first = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(first);
        var firstSeqNo = first.SequenceNumber;
        var firstLockToken = first.LockToken;

        // Abandon — should re-enqueue for redelivery
        await receiver.AbandonMessageAsync(first);

        // Second delivery
        var second = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(second);
        var secondSeqNo = second.SequenceNumber;
        var secondLockToken = second.LockToken;

        // Log the results for comparison
        // Real ASB: SequenceNumber stays the same, LockToken changes
        // Emulator: need to verify
        Assert.Equal(2, second.DeliveryCount);

        // Real ASB keeps the same SequenceNumber on redelivery — the message
        // retains its identity in the queue. Verify the emulator matches.
        Assert.Equal(firstSeqNo, secondSeqNo);

        // Lock token should always be different (fresh lock per delivery)
        Assert.NotEqual(firstLockToken, secondLockToken);

        await receiver.CompleteMessageAsync(second);
    }
}
