using System.Net;
using System.Net.Sockets;
using Azure.Core.Pipeline;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// SQL filters, actions and parameters end to end through the real SDK: rules created with the admin client
/// (HTTP) and with ServiceBusRuleManager (AMQP $management), then routing checked by sending and receiving.
/// Expected errors are the ones real Azure raised for the same calls (gkinsman/AlmostServiceBus#192).
/// </summary>
public class SqlRuleTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();
    private ServiceBusAdministrationClient _admin = null!;
    private ServiceBusClient _client = null!;

    public async Task InitializeAsync()
    {
        await _fixture.StartAsync();

        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (context, ct) =>
            {
                var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
                await socket.ConnectAsync(IPAddress.Loopback, context.DnsEndPoint.Port, ct);
                return new NetworkStream(socket, ownsSocket: true);
            }
        };
        _admin = new ServiceBusAdministrationClient(
            _fixture.ConnectionString,
            new ServiceBusAdministrationClientOptions { Transport = new HttpClientTransport(new HttpClient(handler)) });

        // No retries: Azure answers an invalid rule-manager rule with a transient GeneralError.
        _client = new ServiceBusClient(_fixture.ConnectionString, new ServiceBusClientOptions
        {
            TransportType = ServiceBusTransportType.AmqpTcp,
            RetryOptions = new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) },
        });
    }

    public async Task DisposeAsync()
    {
        await _client.DisposeAsync();
        await _fixture.DisposeAsync();
    }

    private async Task<string> CreateTopicAsync()
    {
        var topic = "sql-" + Guid.NewGuid().ToString("N")[..8];
        await _admin.CreateTopicAsync(topic);
        return topic;
    }

    private Task CreateSubscriptionAsync(string topic, CreateRuleOptions rule) =>
        _admin.CreateSubscriptionAsync(new CreateSubscriptionOptions(topic, "sub"), rule);

    private static Dictionary<string, object> P(params (string Key, object Value)[] properties) =>
        properties.ToDictionary(p => p.Key, p => p.Value);

    private async Task SendAsync(string topic, params Dictionary<string, object>[] messages)
    {
        await using var sender = _client.CreateSender(topic);
        foreach (var properties in messages)
        {
            var message = new ServiceBusMessage("body");
            foreach (var (key, value) in properties) message.ApplicationProperties[key] = value;
            await sender.SendMessageAsync(message);
        }
    }

    private async Task<IReadOnlyList<ServiceBusReceivedMessage>> ReceiveAllAsync(string topic, SubQueue subQueue = SubQueue.None)
    {
        await using var receiver = _client.CreateReceiver(topic, "sub", new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete, SubQueue = subQueue
        });
        var all = new List<ServiceBusReceivedMessage>();
        while (true)
        {
            var batch = await receiver.ReceiveMessagesAsync(10, TimeSpan.FromSeconds(all.Count == 0 ? 2 : 1));
            if (batch.Count == 0) return all;
            all.AddRange(batch);
        }
    }

    // ── Admin client (HTTP) ──────────────────────────────────────────────────

    [Fact]
    public async Task AdminClient_IssueRepro_RoutesOnlyCreated()
    {
        var topic = await CreateTopicAsync();
        await CreateSubscriptionAsync(topic, new CreateRuleOptions("r", new SqlRuleFilter("\"message-type\" IN ('Created', 'Updated')")));

        await SendAsync(topic, P(("message-type", "Created")), P(("message-type", "Unrelated")));

        var received = Assert.Single(await ReceiveAllAsync(topic));
        Assert.Equal("Created", received.ApplicationProperties["message-type"]);
    }

    [Fact]
    public async Task AdminClient_ParametersAndAction()
    {
        var topic = await CreateTopicAsync();
        var filter = new SqlRuleFilter("amount > @min");
        filter.Parameters["@min"] = 10L;
        await CreateSubscriptionAsync(topic, new CreateRuleOptions("r", filter) { Action = new SqlRuleAction("SET user.routed = 'yes'") });

        await SendAsync(topic, P(("amount", 11)), P(("amount", 5)));

        var received = Assert.Single(await ReceiveAllAsync(topic));
        Assert.Equal(11, received.ApplicationProperties["amount"]);
        Assert.Equal("yes", received.ApplicationProperties["routed"]);
        Assert.Equal("r", received.ApplicationProperties["RuleName"]);

        var rule = (await _admin.GetRuleAsync(topic, "sub", "r")).Value;
        Assert.Equal(10L, ((SqlRuleFilter)rule.Filter).Parameters["@min"]);
        Assert.Equal("SET user.routed = 'yes'", ((SqlRuleAction)rule.Action).SqlExpression);
    }

    [Fact]
    public async Task AdminClient_TimeSpanPropertyFilter_AndActionWritingTimeSpan()
    {
        var topic = await CreateTopicAsync();
        var filter = new SqlRuleFilter("ts = @t");
        filter.Parameters["@t"] = TimeSpan.FromHours(1);
        await CreateSubscriptionAsync(topic, new CreateRuleOptions("r", filter) { Action = new SqlRuleAction("SET user.copy = ts") });

        await SendAsync(topic, P(("ts", TimeSpan.FromHours(1))), P(("ts", TimeSpan.FromHours(2))));

        var received = Assert.Single(await ReceiveAllAsync(topic));
        Assert.Equal(TimeSpan.FromHours(1), received.ApplicationProperties["copy"]);
    }

    [Fact]
    public async Task AdminClient_InvalidExpression_ThrowsArgumentException()
    {
        var topic = await CreateTopicAsync();
        await _admin.CreateSubscriptionAsync(topic, "sub");

        var ex = await Assert.ThrowsAsync<ArgumentException>(() =>
            _admin.CreateRuleAsync(topic, "sub", new CreateRuleOptions("bad", new SqlRuleFilter("color IN ('a'"))));

        Assert.StartsWith("There was an error parsing the SQL expression.", ex.Message);
    }

    [Fact]
    public async Task AdminClient_DuplicateRule_ThrowsEntityAlreadyExists()
    {
        var topic = await CreateTopicAsync();
        await _admin.CreateSubscriptionAsync(topic, "sub");
        await _admin.CreateRuleAsync(topic, "sub", new CreateRuleOptions("dup", new SqlRuleFilter("1=1")));

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() =>
            _admin.CreateRuleAsync(topic, "sub", new CreateRuleOptions("dup", new SqlRuleFilter("1=1"))));

        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, ex.Reason);
    }

    [Fact]
    public async Task AdminClient_FilterException_DeadLettersWithReason()
    {
        var topic = await CreateTopicAsync();
        await CreateSubscriptionAsync(topic, new CreateRuleOptions("r", new SqlRuleFilter("color > 1")));

        await SendAsync(topic, P(("color", "red")));

        Assert.Empty(await ReceiveAllAsync(topic));
        var dead = Assert.Single(await ReceiveAllAsync(topic, SubQueue.DeadLetter));
        Assert.Equal("FilterException", dead.DeadLetterReason);
        Assert.Equal("Operator '>' cannot be applied to operands of type 'string' and 'long'", dead.DeadLetterErrorDescription);
        Assert.Equal("FilteringEngine", dead.ApplicationProperties["DeadLetterSource"]);
    }

    // ── Rule manager (AMQP) ──────────────────────────────────────────────────

    [Fact]
    public async Task RuleManager_CreateListDelete()
    {
        var topic = await CreateTopicAsync();
        await _admin.CreateSubscriptionAsync(topic, "sub");
        await using var rules = _client.CreateRuleManager(topic, "sub");

        await rules.DeleteRuleAsync("$Default");
        await rules.CreateRuleAsync(new CreateRuleOptions("r", new SqlRuleFilter("[NServiceBus.EnclosedMessageTypes] LIKE '%Ns.MyEvent%'"))
        {
            Action = new SqlRuleAction("SET sys.Label = 'nsb'")
        });

        var listed = new List<RuleProperties>();
        await foreach (var rule in rules.GetRulesAsync()) listed.Add(rule);
        var only = Assert.Single(listed);
        Assert.Equal("r", only.Name);
        Assert.Equal("[NServiceBus.EnclosedMessageTypes] LIKE '%Ns.MyEvent%'", Assert.IsType<SqlRuleFilter>(only.Filter).SqlExpression);
        Assert.Equal("SET sys.Label = 'nsb'", Assert.IsType<SqlRuleAction>(only.Action).SqlExpression);

        await SendAsync(topic,
            P(("NServiceBus.EnclosedMessageTypes", "Ns.MyEvent, Asm;Ns.IEvent, Asm")),
            P(("NServiceBus.EnclosedMessageTypes", "Ns.Other, Asm")));
        var received = Assert.Single(await ReceiveAllAsync(topic));
        Assert.Equal("nsb", received.Subject);

        await rules.DeleteRuleAsync("r");
        await SendAsync(topic, P(("NServiceBus.EnclosedMessageTypes", "Ns.MyEvent")));
        Assert.Empty(await ReceiveAllAsync(topic));
    }

    [Fact]
    public async Task RuleManager_TrueFilter_ReadsBackAsSql11()
    {
        var topic = await CreateTopicAsync();
        await _admin.CreateSubscriptionAsync(topic, "sub");
        await using var rules = _client.CreateRuleManager(topic, "sub");

        await rules.CreateRuleAsync("t", new TrueRuleFilter());

        var listed = new Dictionary<string, RuleProperties>();
        await foreach (var rule in rules.GetRulesAsync()) listed[rule.Name] = rule;
        Assert.IsType<TrueRuleFilter>(listed["$Default"].Filter);
        Assert.Equal("1=1", Assert.IsType<SqlRuleFilter>(listed["t"].Filter).SqlExpression);
    }

    [Fact]
    public async Task RuleManager_InvalidExpression_ThrowsGeneralError()
    {
        var topic = await CreateTopicAsync();
        await _admin.CreateSubscriptionAsync(topic, "sub");
        await using var rules = _client.CreateRuleManager(topic, "sub");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => rules.CreateRuleAsync("bad", new SqlRuleFilter("color IN ('a'")));

        Assert.Equal(ServiceBusFailureReason.GeneralError, ex.Reason);
    }

    [Fact]
    public async Task RuleManager_Duplicate_EntityAlreadyExists()
    {
        var topic = await CreateTopicAsync();
        await _admin.CreateSubscriptionAsync(topic, "sub");
        await using var rules = _client.CreateRuleManager(topic, "sub");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => rules.CreateRuleAsync("$Default", new TrueRuleFilter()));

        Assert.Equal(ServiceBusFailureReason.MessagingEntityAlreadyExists, ex.Reason);
    }

    [Fact]
    public async Task RuleManager_DeleteMissing_EntityNotFound()
    {
        var topic = await CreateTopicAsync();
        await _admin.CreateSubscriptionAsync(topic, "sub");
        await using var rules = _client.CreateRuleManager(topic, "sub");

        var ex = await Assert.ThrowsAsync<ServiceBusException>(() => rules.DeleteRuleAsync("nope"));

        Assert.Equal(ServiceBusFailureReason.MessagingEntityNotFound, ex.Reason);
    }
}
