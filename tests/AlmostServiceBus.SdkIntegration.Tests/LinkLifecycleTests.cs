using System.Collections.Concurrent;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// What happens at the edges of a receive link's life: an accept-next-session that the client gives
/// up on, and a link that goes away with messages still in flight. Both used to destabilise every
/// other link on the same connection, which is how the OrderFlow demo "seized up" under load.
/// </summary>
public class LinkLifecycleTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    private ServiceBusClient CreateClient(TimeSpan tryTimeout) => new(
        _fixture.ConnectionString,
        new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = tryTimeout },
        });

    /// <summary>
    /// A session processor with more concurrent session slots than there are sessions keeps
    /// long-polling for "next available session". The SDK tells the service how long it will wait;
    /// the service must answer before that or the client closes the AMQP session and our late
    /// Attach kills the whole connection ("The session channel 'N' cannot be found").
    /// </summary>
    [Fact]
    public async Task SessionProcessor_IdleAcceptSlots_DoNotKillTheConnection()
    {
        const string queueName = "session-idle-accept";
        var queue = _fixture.GetNamespaceContext().CreateQueue(queueName);
        queue.RequiresSession = true;

        // Short timeout so the client's accept attempts cycle several times during the test.
        var tryTimeout = TimeSpan.FromSeconds(3);
        await using var client = CreateClient(tryTimeout);

        // Two sessions with traffic, eight slots: six slots are always idle-polling.
        var sender = client.CreateSender(queueName);
        var completed = new ConcurrentBag<string>();
        var errors = new ConcurrentBag<ProcessErrorEventArgs>();

        await using var processor = client.CreateSessionProcessor(queueName, new ServiceBusSessionProcessorOptions
        {
            MaxConcurrentSessions = 8,
            MaxConcurrentCallsPerSession = 1,
            AutoCompleteMessages = false,
        });
        processor.ProcessMessageAsync += async args =>
        {
            await args.CompleteMessageAsync(args.Message);
            completed.Add(args.Message.Body.ToString());
        };
        processor.ProcessErrorAsync += args =>
        {
            errors.Add(args);
            return Task.CompletedTask;
        };
        await processor.StartProcessingAsync();

        // Keep a trickle of traffic flowing for well over three accept-timeout cycles.
        var deadline = DateTime.UtcNow + tryTimeout * 4;
        var sent = 0;
        while (DateTime.UtcNow < deadline)
        {
            await sender.SendMessageAsync(new ServiceBusMessage($"m{sent}") { SessionId = sent % 2 == 0 ? "A" : "B" });
            sent++;
            await Task.Delay(500);
        }

        await processor.StopProcessingAsync();

        var connectionErrors = errors
            .Where(e => e.Exception.Message.Contains("session channel", StringComparison.OrdinalIgnoreCase)
                     || e.Exception is ServiceBusException { Reason: ServiceBusFailureReason.ServiceCommunicationProblem })
            .Select(e => $"{e.ErrorSource}: {e.Exception.Message}")
            .ToList();

        Assert.True(connectionErrors.Count == 0, "Connection-level errors:\n" + string.Join("\n", connectionErrors));
        Assert.Equal(sent, completed.Count);
    }

    /// <summary>
    /// The client explicitly gives up waiting for a session (its TryTimeout elapses with no
    /// messages anywhere). It must surface as a plain ServiceTimeout, and a later accept on the
    /// same client must still work — i.e. the connection survived.
    /// </summary>
    [Fact]
    public async Task AcceptNextSession_ClientTimeout_IsServiceTimeout_AndConnectionSurvives()
    {
        const string queueName = "session-accept-timeout";
        var queue = _fixture.GetNamespaceContext().CreateQueue(queueName);
        queue.RequiresSession = true;

        // Generous enough for the first connection of the process (SASL + CBS) to fit inside it.
        await using var client = CreateClient(TimeSpan.FromSeconds(6));

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => client.AcceptNextSessionAsync(queueName));
        Assert.True(ex.Reason == ServiceBusFailureReason.ServiceTimeout, ex.ToString());
        // The emulator must be the one to give up (it was told the budget via com.microsoft:timeout);
        // a client-side timeout would carry the SDK's own "did not complete within the allocated time" text.
        Assert.Contains("No sessions are available", ex.Message);

        await client.CreateSender(queueName).SendMessageAsync(new ServiceBusMessage("hi") { SessionId = "S" });
        await using var receiver = await client.AcceptNextSessionAsync(queueName);
        Assert.Equal("S", receiver.SessionId);
        var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(msg);
    }

    /// <summary>
    /// A receiver goes away with a message locked but unsettled. Real Service Bus keeps the lock
    /// until it expires; it does not hand the message straight to the next receiver. AMQPNetLite
    /// synthesises a Released outcome for the unsettled delivery when it aborts the link, and we
    /// used to treat that as an abandon — redelivering a message whose handler was still running.
    /// </summary>
    [Fact]
    public async Task ReceiverClosedWithUnsettledMessage_MessageStaysLocked()
    {
        const string queueName = "link-teardown-lock";
        var queue = _fixture.GetNamespaceContext().CreateQueue(queueName);
        queue.LockDuration = TimeSpan.FromSeconds(30);

        await using var client = CreateClient(TimeSpan.FromSeconds(5));
        await client.CreateSender(queueName).SendMessageAsync(new ServiceBusMessage("in-flight"));

        var receiver = client.CreateReceiver(queueName, new ServiceBusReceiverOptions { PrefetchCount = 0 });
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal(0, queue.MessageCount);

        // Tear the link down without settling.
        await receiver.CloseAsync();

        // Give any (wrongly) synthesised abandon time to re-enqueue (it used a 1s delay).
        await Task.Delay(TimeSpan.FromSeconds(2));

        Assert.Equal(0, queue.MessageCount);
        Assert.True(queue.IsLockValid(received.LockToken), "the lock should survive the link closing");

        var second = client.CreateReceiver(queueName, new ServiceBusReceiverOptions { PrefetchCount = 0 });
        var redelivered = await second.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
        Assert.Null(redelivered);
    }

    /// <summary>
    /// Same as above for a session receiver: closing the link releases the session lock but the
    /// unsettled message is only redelivered when a receiver accepts the session again.
    /// </summary>
    [Fact]
    public async Task SessionReceiverClosedWithUnsettledMessage_RedeliveredOnlyToNextSessionAccept()
    {
        const string queueName = "session-teardown-lock";
        var queue = _fixture.GetNamespaceContext().CreateQueue(queueName);
        queue.RequiresSession = true;
        queue.LockDuration = TimeSpan.FromSeconds(30);

        await using var client = CreateClient(TimeSpan.FromSeconds(5));
        await client.CreateSender(queueName).SendMessageAsync(new ServiceBusMessage("in-flight") { SessionId = "S" });

        var receiver = await client.AcceptSessionAsync(queueName, "S");
        var received = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(received);
        Assert.Equal(0, queue.MessageCount);

        await receiver.CloseAsync();
        await Task.Delay(TimeSpan.FromSeconds(2));

        // Not re-enqueued by the teardown itself...
        Assert.Equal(0, queue.MessageCount);

        // ...but available again to the next receiver that takes the session.
        await using var second = await client.AcceptSessionAsync(queueName, "S");
        var redelivered = await second.ReceiveMessageAsync(TimeSpan.FromSeconds(5));
        Assert.NotNull(redelivered);
        Assert.Equal("in-flight", redelivered.Body.ToString());
        Assert.Equal(2, redelivered.DeliveryCount);
        await second.CompleteMessageAsync(redelivered);
        Assert.Equal(0, queue.MessageCount);
        Assert.Equal(1, queue.ConsumedCount);
    }
}
