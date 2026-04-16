using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Tests that reproduce the Wolverine tracking_correlation_id_on_everything failure.
/// The issue: when 2+ messages are sent via ServiceBusMessageBatch and received by
/// a ServiceBusProcessor, the processor releases (abandons) messages instead of
/// passing them to the handler callback.
/// </summary>
public class ProcessorBatchTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    private ServiceBusClient CreateClient()
    {
        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
        return new ServiceBusClient(cs, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) }
        });
    }

    [Fact]
    public async Task SingleMessage_Processor_ReceivesAndCompletes()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("proc-single");

        await using var client = CreateClient();
        var sender = client.CreateSender("proc-single");
        await sender.SendMessageAsync(new ServiceBusMessage("hello") { Subject = "TestType" });
        await sender.CloseAsync();

        var received = new TaskCompletionSource<string>();
        var processor = client.CreateProcessor("proc-single");
        processor.ProcessMessageAsync += args =>
        {
            received.TrySetResult(args.Message.Subject);
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();
        var subject = await received.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await processor.StopProcessingAsync();

        Assert.Equal("TestType", subject);
    }

    [Fact]
    public async Task TwoMessages_SentIndividually_Processor_ReceivesBoth()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("proc-two-individual");

        await using var client = CreateClient();
        var sender = client.CreateSender("proc-two-individual");
        await sender.SendMessageAsync(new ServiceBusMessage("msg1") { Subject = "Type1" });
        await sender.SendMessageAsync(new ServiceBusMessage("msg2") { Subject = "Type2" });
        await sender.CloseAsync();

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();
        var processor = client.CreateProcessor("proc-two-individual");
        processor.ProcessMessageAsync += args =>
        {
            received.Add(args.Message.Subject);
            if (received.Count >= 2) allReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();
        await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(15));
        await processor.StopProcessingAsync();

        Assert.Equal(2, received.Count);
        Assert.Contains("Type1", received);
        Assert.Contains("Type2", received);
    }

    [Fact]
    public async Task TwoMessages_SentAsBatch_Processor_ReceivesBoth()
    {
        // This is what Wolverine's BatchedSender does — sends via ServiceBusMessageBatch.
        // The Azure SDK encodes batches as a single AMQP transfer with Data[] body.
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("proc-two-batch");

        await using var client = CreateClient();
        var sender = client.CreateSender("proc-two-batch");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "BatchType1", MessageId = "batch-msg-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "BatchType2", MessageId = "batch-msg-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();
        var processor = client.CreateProcessor("proc-two-batch");
        processor.ProcessMessageAsync += args =>
        {
            received.Add(args.Message.Subject);
            if (received.Count >= 2) allReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"[PROC-ERROR] {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();

        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/2 messages: [{string.Join(", ", received)}]");
        Assert.Equal(2, received.Count);
    }

    [Fact]
    public async Task TwoMessages_SentAsBatch_Receiver_ReceivesBoth()
    {
        // Same as above but with ServiceBusReceiver instead of Processor.
        // This tests if the issue is Processor-specific.
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("recv-two-batch");

        await using var client = CreateClient();
        var sender = client.CreateSender("recv-two-batch");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "RBatchType1", MessageId = "rbatch-msg-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "RBatchType2", MessageId = "rbatch-msg-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        var receiver = client.CreateReceiver("recv-two-batch");
        var messages = new List<ServiceBusReceivedMessage>();

        // Receive up to 2 messages with retries
        var sw = Stopwatch.StartNew();
        while (messages.Count < 2 && sw.Elapsed < TimeSpan.FromSeconds(15))
        {
            var msg = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));
            if (msg != null)
            {
                messages.Add(msg);
                await receiver.CompleteMessageAsync(msg);
            }
        }

        await receiver.CloseAsync();

        Assert.Equal(2, messages.Count);
        Assert.Contains(messages, m => m.Subject == "RBatchType1");
        Assert.Contains(messages, m => m.Subject == "RBatchType2");
    }

    [Fact]
    public async Task FiveMessages_SentAsBatch_Processor_ReceivesAll()
    {
        // Larger batch to stress-test the processor interaction
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("proc-five-batch");

        await using var client = CreateClient();
        var sender = client.CreateSender("proc-five-batch");

        using var batch = await sender.CreateMessageBatchAsync();
        for (int i = 0; i < 5; i++)
            Assert.True(batch.TryAddMessage(new ServiceBusMessage($"msg-{i}") { Subject = $"Type{i}", MessageId = $"five-msg-{i}" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();
        var processor = client.CreateProcessor("proc-five-batch");
        processor.ProcessMessageAsync += args =>
        {
            received.Add(args.Message.Subject);
            if (received.Count >= 5) allReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args => Task.CompletedTask;

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(30)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/5 messages: [{string.Join(", ", received)}]");
    }

    [Fact]
    public async Task TwoMessages_SentAsBatch_Processor_WithExplicitComplete_ReceivesBoth()
    {
        // Test with AutoCompleteMessages=false (like Wolverine should probably use)
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("proc-batch-explicit");

        await using var client = CreateClient();
        var sender = client.CreateSender("proc-batch-explicit");

        using var batch = await sender.CreateMessageBatchAsync();
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "ExType1", MessageId = "ex-msg-1" }));
        Assert.True(batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "ExType2", MessageId = "ex-msg-2" }));
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();
        var processor = client.CreateProcessor("proc-batch-explicit", new ServiceBusProcessorOptions
        {
            AutoCompleteMessages = false,
            MaxConcurrentCalls = 1,
            PrefetchCount = 0
        });
        processor.ProcessMessageAsync += async args =>
        {
            received.Add(args.Message.Subject);
            await args.CompleteMessageAsync(args.Message);
            if (received.Count >= 2) allReceived.TrySetResult();
        };
        processor.ProcessErrorAsync += args =>
        {
            Console.WriteLine($"[PROC-ERROR-EXPLICIT] {args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        Assert.True(allReceived.Task.IsCompletedSuccessfully,
            $"Only received {received.Count}/2 messages: [{string.Join(", ", received)}]");
    }
}
