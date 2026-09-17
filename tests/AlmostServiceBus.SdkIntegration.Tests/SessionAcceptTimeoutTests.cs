using System.Collections.Concurrent;
using System.Diagnostics;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.TestHost;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// Reproduces the connection teardown seen in the OrderFlowDemo under load:
/// a session processor whose pumps wait for a session that never arrives
/// (MassTransit runs 16+ session pumps against a handful of warehouse sessions).
///
/// The Azure SDK gives up on <c>AcceptNextSession</c> after <c>TryTimeout</c> and
/// sends Detach + End for the still-pending link. If the emulator then completes the
/// pending attach anyway, the Attach frame lands on a channel the client has already
/// removed and the client closes the whole connection with
/// "amqp:not-found — The session channel 'N' cannot be found". Every other receiver
/// on that connection dies with it.
/// </summary>
public class SessionAcceptTimeoutTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private readonly ConcurrentQueue<string> _emulatorWarnings = new();
    private ILoggerFactory? _previousFactory;

    private readonly System.Text.StringBuilder _frames = new();

    public async Task InitializeAsync()
    {
        Amqp.Trace.TraceLevel = Amqp.TraceLevel.Frame;
        Amqp.Trace.TraceListener = (level, format, args) =>
        {
            try
            {
                var line = args is { Length: > 0 } ? string.Format(format, args) : format;
                lock (_frames) _frames.AppendLine($"{DateTime.UtcNow:HH:mm:ss.fff} T{Environment.CurrentManagedThreadId,-3} [{level}] {line}");
            }
            catch { }
        };
        _previousFactory = AmqpLog.Factory;
        AmqpLog.Factory = LoggerFactory.Create(b => b
            .SetMinimumLevel(LogLevel.Warning)
            .AddProvider(new QueueLoggerProvider(_emulatorWarnings)));
        await _fixture.StartAsync();
    }

    public async Task DisposeAsync()
    {
        Amqp.Trace.TraceLevel = Amqp.TraceLevel.Error;
        Amqp.Trace.TraceListener = null;
        await _fixture.DisposeAsync();
        AmqpLog.Factory = _previousFactory ?? Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance;
    }

    private ServiceBusClient CreateClient(TimeSpan tryTimeout)
    {
        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
        return new ServiceBusClient(cs, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = tryTimeout }
        });
    }

    [Fact]
    public async Task SessionProcessor_WaitingForSessionsThatNeverArrive_DoesNotKillConnection()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        var sessionQueue = context.CreateQueue("accept-timeout-sessions");
        sessionQueue.RequiresSession = true;
        context.CreateQueue("accept-timeout-work");

        // Short TryTimeout so the SDK gives up on AcceptNextSession several times
        // during the test. One client => one AMQP connection shared by both processors.
        await using var client = CreateClient(TimeSpan.FromSeconds(5));

        var sessionErrors = new ConcurrentBag<Exception>();
        await using var sessionProcessor = client.CreateSessionProcessor("accept-timeout-sessions",
            new ServiceBusSessionProcessorOptions
            {
                MaxConcurrentSessions = 3,
                MaxConcurrentCallsPerSession = 1,
                AutoCompleteMessages = true,
            });
        sessionProcessor.ProcessMessageAsync += _ => Task.CompletedTask;
        sessionProcessor.ProcessErrorAsync += args =>
        {
            sessionErrors.Add(args.Exception);
            return Task.CompletedTask;
        };

        var workErrors = new ConcurrentBag<Exception>();
        var received = new ConcurrentBag<string>();
        await using var workProcessor = client.CreateProcessor("accept-timeout-work",
            new ServiceBusProcessorOptions { AutoCompleteMessages = true, MaxConcurrentCalls = 1 });
        workProcessor.ProcessMessageAsync += args =>
        {
            received.Add(args.Message.MessageId);
            return Task.CompletedTask;
        };
        workProcessor.ProcessErrorAsync += args =>
        {
            workErrors.Add(args.Exception);
            return Task.CompletedTask;
        };

        await sessionProcessor.StartProcessingAsync();
        await workProcessor.StartProcessingAsync();

        // Keep the non-session queue busy while the session pumps time out repeatedly.
        var sender = client.CreateSender("accept-timeout-work");
        var sent = new List<string>();
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(12))
        {
            var id = Guid.NewGuid().ToString("N");
            await sender.SendMessageAsync(new ServiceBusMessage("work") { MessageId = id });
            sent.Add(id);
            await Task.Delay(250);
        }

        // Give the last message time to arrive, then stop.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (received.Count < sent.Count && DateTime.UtcNow < deadline)
            await Task.Delay(100);

        await workProcessor.StopProcessingAsync();
        await sessionProcessor.StopProcessingAsync();

        var connectionClosedWithError = _emulatorWarnings
            .Where(w => w.Contains("AMQP connection closed with error", StringComparison.Ordinal))
            .ToList();

        Assert.True(connectionClosedWithError.Count == 0,
            $"Emulator saw {connectionClosedWithError.Count} connection(s) closed with error. First: {connectionClosedWithError.FirstOrDefault()}");
        Assert.True(workErrors.IsEmpty,
            $"Non-session processor reported {workErrors.Count} error(s). First: {workErrors.FirstOrDefault()?.Message}");
        Assert.True(sessionErrors.IsEmpty,
            $"Session processor reported {sessionErrors.Count} error(s). First: {sessionErrors.FirstOrDefault()?.Message}");
        Assert.Equal(sent.Count, received.Count);
    }

    [Fact]
    public async Task AcceptNextSession_EmptyQueue_FailsWithServiceTimeoutNearTryTimeout()
    {
        var context = _fixture.GetDefaultNamespaceContext();
        var sessionQueue = context.CreateQueue("accept-timeout-empty");
        sessionQueue.RequiresSession = true;

        await using var client = CreateClient(TimeSpan.FromSeconds(5));

        var stopwatch = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<ServiceBusException>(() =>
            client.AcceptNextSessionAsync("accept-timeout-empty"));
        stopwatch.Stop();

        Assert.Equal(ServiceBusFailureReason.ServiceTimeout, ex.Reason);
        string frames; lock (_frames) frames = _frames.ToString();
        // The AMQP exchange completes at ~TryTimeout (the emulator answers at the client's
        // com.microsoft:timeout); the SDK then spends ~2s on its own teardown before it
        // surfaces the exception, so allow a generous margin on top.
        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(9),
            $"AcceptNextSession took {stopwatch.Elapsed.TotalSeconds:F1}s for a 5s TryTimeout.\nWarnings:\n{string.Join("\n", _emulatorWarnings)}\nFrames:\n{frames}");

        // Sending on the same connection afterwards must still work — the connection
        // must not have been torn down by the timed-out accept.
        context.CreateQueue("accept-timeout-after");
        var sender = client.CreateSender("accept-timeout-after");
        await sender.SendMessageAsync(new ServiceBusMessage("still alive"));
        Assert.Equal(1, context.GetQueue("accept-timeout-after")!.MessageCount);

        var connectionClosedWithError = _emulatorWarnings
            .Where(w => w.Contains("AMQP connection closed with error", StringComparison.Ordinal))
            .ToList();
        Assert.True(connectionClosedWithError.Count == 0,
            $"Emulator saw a connection closed with error: {connectionClosedWithError.FirstOrDefault()}");
    }

    private sealed class QueueLoggerProvider(ConcurrentQueue<string> sink) : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new QueueLogger(categoryName, sink);
        public void Dispose() { }

        private sealed class QueueLogger(string category, ConcurrentQueue<string> sink) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;
            public void Log<TState>(LogLevel logLevel, Microsoft.Extensions.Logging.EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            {
                if (!IsEnabled(logLevel)) return;
                sink.Enqueue($"[{logLevel}] {category}: {formatter(state, exception)}");
            }
        }
    }
}
