using Amqp;
using Amqp.Framing;
using Amqp.Types;
using AlmostServiceBus.Core.Amqp;

namespace AlmostServiceBus.Tests.Amqp;

/// <summary>
/// The Python azure-servicebus client uses its own AMQP implementation (pyamqp), not the one the
/// .NET client uses. These tests cover the three places where the emulator made an assumption
/// that only holds for the .NET client.
/// </summary>
public class PythonClientCompatibilityTests
{
    [Fact]
    public void EmitTrailingFields_GivesAnOpenItsLastField()
    {
        var open = new Open();

        GuidDeliveryTagHandler.EmitTrailingFields(open);

        Assert.NotNull(open.Properties);
    }

    [Fact]
    public void EmitTrailingFields_GivesABeginItsLastField()
    {
        var begin = new Begin();

        GuidDeliveryTagHandler.EmitTrailingFields(begin);

        Assert.NotNull(begin.Properties);
    }

    [Fact]
    public void EmitTrailingFields_GivesAnAttachItsLastField()
    {
        var attach = new Attach();

        GuidDeliveryTagHandler.EmitTrailingFields(attach);

        Assert.NotNull(attach.Properties);
    }

    [Fact]
    public void EmitTrailingFields_KeepsPropertiesThatAreAlreadySet()
    {
        var properties = new Fields { [new Symbol("com.example")] = "value" };
        var open = new Open { Properties = properties };

        GuidDeliveryTagHandler.EmitTrailingFields(open);

        Assert.Same(properties, open.Properties);
    }

    [Fact]
    public void EmitTrailingFields_IgnoresAnythingElse()
    {
        GuidDeliveryTagHandler.EmitTrailingFields(null);
        GuidDeliveryTagHandler.EmitTrailingFields(new Flow());
    }

    [Fact]
    public void CorrelateTo_KeepsAUuidMessageId()
    {
        var messageId = Guid.NewGuid();
        var request = new Message();
        request.Properties = new Properties();
        request.Properties.SetMessageId(messageId);

        var reply = AmqpIdentifiers.CorrelateTo(request);

        Assert.Equal(messageId, reply.GetCorrelationId());
    }

    [Fact]
    public void CorrelateTo_KeepsAStringMessageId()
    {
        var request = new Message { Properties = new Properties { MessageId = "request-1" } };

        var reply = AmqpIdentifiers.CorrelateTo(request);

        Assert.Equal("request-1", reply.GetCorrelationId());
    }

    [Fact]
    public void CorrelateTo_LeavesTheCorrelationIdUnsetWhenTheRequestHasNoMessageId()
    {
        Assert.Null(AmqpIdentifiers.CorrelateTo(new Message()).GetCorrelationId());
        Assert.Null(AmqpIdentifiers.CorrelateTo(new Message { Properties = new Properties() })
            .GetCorrelationId());
    }

    [Fact]
    public void MessageIdAsText_ReadsAUuidWithoutThrowing()
    {
        var messageId = Guid.NewGuid();
        var properties = new Properties();
        properties.SetMessageId(messageId);

        Assert.Equal(messageId.ToString(), AmqpIdentifiers.MessageIdAsText(properties));
    }

    [Fact]
    public void CorrelationIdAsText_ReadsAUuidWithoutThrowing()
    {
        var correlationId = Guid.NewGuid();
        var properties = new Properties();
        properties.SetCorrelationId(correlationId);

        Assert.Equal(correlationId.ToString(), AmqpIdentifiers.CorrelationIdAsText(properties));
    }

    [Theory]
    // What the .NET client sends.
    [InlineData("my-queue", "my-queue")]
    [InlineData("/my-queue", "my-queue")]
    [InlineData("my-topic/Subscriptions/my-subscription", "my-topic/Subscriptions/my-subscription")]
    // What the Python client sends.
    [InlineData("amqps://localhost:5673/my-queue", "my-queue")]
    [InlineData(
        "amqps://localhost:5673/my-topic/Subscriptions/my-subscription",
        "my-topic/Subscriptions/my-subscription")]
    [InlineData("amqp://ns.servicebus.windows.net/my-queue/$management", "my-queue/$management")]
    // A URI with no path at all leaves no entity name.
    [InlineData("amqps://localhost:5673", "")]
    public void NormaliseAddress_ReducesAnAddressToTheEntityPath(string address, string expected) =>
        Assert.Equal(expected, ServiceBusLinkProcessor.NormaliseAddress(address));

    [Fact]
    public void NormaliseAddress_PassesNullAndEmptyThrough()
    {
        Assert.Null(ServiceBusLinkProcessor.NormaliseAddress(null));
        Assert.Equal(string.Empty, ServiceBusLinkProcessor.NormaliseAddress(string.Empty));
    }
}
