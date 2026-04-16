using System.Collections.Concurrent;
using System.Text;
using Amqp;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Diagnostic test that enables AMQPNetLite frame-level tracing to capture
/// the exact AMQP protocol exchange during batch message + processor interaction.
/// </summary>
public class AmqpFrameTraceTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private readonly StringBuilder _traceLog = new();

    public async Task InitializeAsync()
    {
        // Enable AMQPNetLite frame tracing
        Trace.TraceLevel = TraceLevel.Frame;
        Trace.TraceListener = (level, format, args) =>
        {
            var line = args.Length > 0 ? string.Format(format, args) : format;
            lock (_traceLog) _traceLog.AppendLine($"[{level}] {line}");
        };

        await _fixture.StartAsync();
    }

    public async Task DisposeAsync()
    {
        Trace.TraceLevel = TraceLevel.Error;
        Trace.TraceListener = null;
        await _fixture.DisposeAsync();
    }

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
    public async Task TraceBatchProcessorFrames_TwoMessages()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        context.CreateQueue("trace-batch");

        await using var client = CreateClient();
        var sender = client.CreateSender("trace-batch");

        using var batch = await sender.CreateMessageBatchAsync();
        batch.TryAddMessage(new ServiceBusMessage("msg1") { Subject = "Trace1", MessageId = "trace-1" });
        batch.TryAddMessage(new ServiceBusMessage("msg2") { Subject = "Trace2", MessageId = "trace-2" });
        await sender.SendMessagesAsync(batch);
        await sender.CloseAsync();

        // Clear trace log before receiving to focus on delivery frames
        lock (_traceLog) _traceLog.Clear();

        var received = new ConcurrentBag<string>();
        var allReceived = new TaskCompletionSource();
        var errors = new ConcurrentBag<string>();

        var processor = client.CreateProcessor("trace-batch");
        processor.ProcessMessageAsync += args =>
        {
            received.Add($"{args.Message.MessageId}:{args.Message.Subject}");
            if (received.Count >= 2) allReceived.TrySetResult();
            return Task.CompletedTask;
        };
        processor.ProcessErrorAsync += args =>
        {
            errors.Add($"{args.Exception.GetType().Name}: {args.Exception.Message}");
            return Task.CompletedTask;
        };

        await processor.StartProcessingAsync();
        var completed = await Task.WhenAny(allReceived.Task, Task.Delay(TimeSpan.FromSeconds(15)));
        await processor.StopProcessingAsync();

        // Dump the AMQP trace for analysis
        string trace;
        lock (_traceLog) trace = _traceLog.ToString();

        // Count transfer and disposition frames
        var transferCount = trace.Split('\n').Count(l => l.Contains("transfer("));
        var dispositionAccepted = trace.Split('\n').Count(l => l.Contains("disposition(") && l.Contains("accepted"));
        var dispositionReleased = trace.Split('\n').Count(l => l.Contains("disposition(") && l.Contains("released"));
        var dispositionLines = trace.Split('\n').Where(l => l.Contains("disposition(")).ToArray();

        // Write summary for test output
        var output = new StringBuilder();
        output.AppendLine($"Messages received: {received.Count}/2 [{string.Join(", ", received)}]");
        output.AppendLine($"Errors: [{string.Join("; ", errors)}]");
        output.AppendLine($"AMQP transfers: {transferCount}");
        output.AppendLine($"AMQP dispositions accepted: {dispositionAccepted}");
        output.AppendLine($"AMQP dispositions released: {dispositionReleased}");
        output.AppendLine("--- Disposition frames ---");
        foreach (var line in dispositionLines.Take(20))
            output.AppendLine(line.Trim());

        Assert.True(allReceived.Task.IsCompletedSuccessfully, output.ToString());
    }
}
