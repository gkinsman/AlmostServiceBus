using Amqp;
using Amqp.Framing;
using Azure.Messaging.ServiceBus;
using AlmostServiceBus.TestHost;

namespace AlmostServiceBus.SdkIntegration.Tests;

/// <summary>
/// A Service Bus batch is one AMQP transfer (message-format 0x80013700) whose body is a
/// Data[] of complete encoded messages. The .NET SDK sends a bare envelope, but the
/// Node.js SDK copies the first message's properties (Subject included) onto it. The
/// emulator used to recognise a batch only by the envelope having no Subject, so a
/// Node.js batch became a single message whose body was the raw encoded batch.
/// </summary>
public class BatchEnvelopeTests : IAsyncLifetime
{
    private readonly ServiceBusEmulatorFixture _fixture = new();

    public async Task InitializeAsync() => await _fixture.StartAsync();
    public async Task DisposeAsync() => await _fixture.DisposeAsync();

    [Fact]
    public async Task BatchEnvelopeWithSubject_IsUnpackedIntoIndividualMessages()
    {
        const string queue = "batch-envelope-subject";
        _fixture.GetDefaultNamespaceContext().CreateQueue(queue);

        var address = new Address("localhost", _fixture.PublicPort, null, null, "/", "AMQP");
        var factory = new ConnectionFactory();
        factory.SASL.Profile = Amqp.Sasl.SaslProfile.Anonymous;
        var connection = await factory.CreateAsync(address);
        var session = new Session(connection);
        var sender = new SenderLink(session, "batch-envelope-sender", queue);

        // Three complete messages, each with its own Subject, packed as consecutive Data
        // sections the way rhea does it.
        var sections = new DataList();
        foreach (var (body, subject) in new[] { ("b1", "OrderPlaced"), ("b2", "OrderPlaced"), ("b3", "OrderShipped") })
        {
            var inner = new Message(new Data { Binary = System.Text.Encoding.UTF8.GetBytes(body) })
            {
                Properties = new Properties { MessageId = body, Subject = subject },
            };
            var encoded = inner.Encode();
            sections.Add(new Data { Binary = encoded.Buffer.AsSpan(encoded.Offset, encoded.Length).ToArray() });
        }

        var envelope = new Message
        {
            Format = 0x80013700,
            BodySection = sections,
            // What the Node.js SDK does: the first message's properties end up on the envelope.
            Properties = new Properties { MessageId = "b1", Subject = "OrderPlaced" },
        };
        await sender.SendAsync(envelope);
        await connection.CloseAsync();

        var cs = $"Endpoint=sb://localhost:{_fixture.PublicPort};SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
        await using var client = new ServiceBusClient(cs);
        var receiver = client.CreateReceiver(queue);
        var received = await receiver.ReceiveMessagesAsync(5, TimeSpan.FromSeconds(5));

        Assert.Equal(["b1", "b2", "b3"], received.Select(m => m.Body.ToString()).ToArray());
        Assert.Equal(["OrderPlaced", "OrderPlaced", "OrderShipped"], received.Select(m => m.Subject).ToArray());
    }
}
