using System.Net;
using System.Net.Sockets;
using AlmostServiceBus.TestHost;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;

namespace AlmostServiceBus.SdkLive.Tests;

/// <summary>
/// Base class for SDK live tests ported from the official Azure SDK test suite.
/// Provides emulator fixture, client creation, entity scoping, and test utilities
/// matching the patterns used by Azure.Messaging.ServiceBus.Tests.
/// </summary>
public abstract class SdkLiveTestBase : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private readonly List<string> _createdQueues = [];
    private readonly List<string> _createdTopics = [];

    protected ServiceBusClient Client { get; private set; } = null!;
    protected ServiceBusAdministrationClient AdminClient { get; private set; } = null!;
    protected string ConnectionString => _fixture.ConnectionString;
    protected int PublicPort => _fixture.PublicPort;

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();

        var clientOptions = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions
            {
                MaxRetries = 3,
                TryTimeout = TimeSpan.FromSeconds(15)
            }
        };

        Client = new ServiceBusClient(ConnectionString, clientOptions);

        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (context, ct) =>
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(IPAddress.Loopback, context.DnsEndPoint.Port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
        };

        var adminOptions = new ServiceBusAdministrationClientOptions();
        adminOptions.Transport = new HttpClientTransport(new HttpClient(handler));
        AdminClient = new ServiceBusAdministrationClient(ConnectionString, adminOptions);
    }

    public async Task DisposeAsync()
    {
        foreach (var topic in _createdTopics)
        {
            try { await AdminClient.DeleteTopicAsync(topic); } catch { }
        }
        foreach (var queue in _createdQueues)
        {
            try { await AdminClient.DeleteQueueAsync(queue); } catch { }
        }

        if (Client is not null)
            await Client.DisposeAsync();

        await _fixture.DisposeAsync();
    }

    /// <summary>
    /// Creates a ServiceBusClient with custom retry settings, connected to the emulator.
    /// </summary>
    protected ServiceBusClient CreateClient(int tryTimeout = 15, int maxRetries = 3)
    {
        var options = new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            CustomEndpointAddress = new Uri($"sb://localhost:{_fixture.PublicPort}"),
            RetryOptions = new ServiceBusRetryOptions
            {
                TryTimeout = TimeSpan.FromSeconds(tryTimeout),
                MaxRetries = maxRetries
            }
        };
        return new ServiceBusClient(ConnectionString, options);
    }

    protected ServiceBusClient CreateNoRetryClient(int tryTimeout = 15) => CreateClient(tryTimeout, 0);

    /// <summary>
    /// Creates a queue and registers it for cleanup. Mirrors ServiceBusScope.CreateWithQueue.
    /// </summary>
    protected async Task<string> CreateQueueAsync(
        bool enableSession = false,
        TimeSpan? lockDuration = null,
        TimeSpan? defaultMessageTimeToLive = null,
        string? callerName = null)
    {
        // Default to a long lock duration (5 minutes) so tests running on slow CI
        // hardware don't hit the 30s default lock expiry during normal receive loops.
        // Tests that specifically exercise lock expiry can override.
        lockDuration ??= TimeSpan.FromMinutes(5);

        var name = $"sdk-{Guid.NewGuid():N}"[..20];
        var options = new CreateQueueOptions(name)
        {
            RequiresSession = enableSession,
        };
        if (lockDuration.HasValue)
            options.LockDuration = lockDuration.Value;
        if (defaultMessageTimeToLive.HasValue)
            options.DefaultMessageTimeToLive = defaultMessageTimeToLive.Value;

        await AdminClient.CreateQueueAsync(options);
        _createdQueues.Add(name);
        return name;
    }

    /// <summary>
    /// Creates a topic with subscriptions and registers for cleanup. Mirrors ServiceBusScope.CreateWithTopic.
    /// </summary>
    protected async Task<(string TopicName, List<string> SubscriptionNames)> CreateTopicAsync(
        bool enableSession = false,
        IEnumerable<string>? subscriptions = null)
    {
        subscriptions ??= ["default-subscription"];
        var topicName = $"sdk-{Guid.NewGuid():N}"[..20];
        await AdminClient.CreateTopicAsync(topicName);
        _createdTopics.Add(topicName);

        var subNames = new List<string>();
        foreach (var sub in subscriptions)
        {
            var subOptions = new CreateSubscriptionOptions(topicName, sub)
            {
                RequiresSession = enableSession
            };
            await AdminClient.CreateSubscriptionAsync(subOptions);
            subNames.Add(sub);
        }

        return (topicName, subNames);
    }

    // ─── Message Utilities (matching ServiceBusTestUtilities) ───

    protected static ServiceBusMessage GetMessage(string? sessionId = null)
    {
        var msg = new ServiceBusMessage(GetRandomBuffer(100))
        {
            Subject = $"test-{Guid.NewGuid()}",
            MessageId = Guid.NewGuid().ToString()
        };
        if (sessionId is not null)
            msg.SessionId = sessionId;
        return msg;
    }

    protected static List<ServiceBusMessage> GetMessages(int count, string? sessionId = null)
    {
        var messages = new List<ServiceBusMessage>();
        for (int i = 0; i < count; i++)
            messages.Add(GetMessage(sessionId));
        return messages;
    }

    protected static ServiceBusMessageBatch AddMessages(ServiceBusMessageBatch batch, int count, string? sessionId = null)
    {
        for (int i = 0; i < count; i++)
            Assert.True(batch.TryAddMessage(GetMessage(sessionId)), "A message was rejected by the batch");
        return batch;
    }

    protected static List<ServiceBusMessage> AddAndReturnMessages(ServiceBusMessageBatch batch, int count, string? sessionId = null)
    {
        var messages = new List<ServiceBusMessage>();
        for (int i = 0; i < count; i++)
        {
            var msg = GetMessage(sessionId);
            Assert.True(batch.TryAddMessage(msg), "A message was rejected by the batch");
            messages.Add(msg);
        }
        return messages;
    }

    protected static byte[] GetRandomBuffer(int size)
    {
        var chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".ToCharArray();
        var random = new Random();
        var buffer = new byte[size];
        random.NextBytes(buffer);
        var text = new byte[size];
        for (int i = 0; i < size; i++)
            text[i] = (byte)chars[buffer[i] % chars.Length];
        return text;
    }

    protected static Task ExceptionHandler(ProcessErrorEventArgs eventArgs)
    {
        Assert.Fail(eventArgs.Exception.ToString());
        return Task.CompletedTask;
    }
}
