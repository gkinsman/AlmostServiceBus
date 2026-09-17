using Amqp.Framing;
using Amqp.Types;
using AlmostServiceBus.Core.Amqp;

namespace AlmostServiceBus.Tests.Amqp;

/// <summary>
/// The Azure SDK tells the service how long to hold an accept-next-session attach via the
/// <c>com.microsoft:timeout</c> attach property (milliseconds), deliberately a little shorter
/// than its own TryTimeout. The emulator must honour it so the service side always answers
/// before the client gives up; the real service caps it at 65 seconds.
/// </summary>
public class SessionAcceptTimeoutResolutionTests
{
    private static Attach AttachWithTimeout(object? value)
    {
        var attach = new Attach { Properties = new Fields() };
        if (value is not null)
            attach.Properties[new Symbol("com.microsoft:timeout")] = value;
        return attach;
    }

    [Fact]
    public void NoProperties_FallsBackToServiceCap()
    {
        var timeout = ServiceBusLinkProcessor.ResolveSessionAcceptWait(new Attach());
        Assert.Equal(ServiceBusLinkProcessor.MaxSessionAcceptWait, timeout);
    }

    [Fact]
    public void MissingTimeoutProperty_FallsBackToServiceCap()
    {
        var timeout = ServiceBusLinkProcessor.ResolveSessionAcceptWait(AttachWithTimeout(null));
        Assert.Equal(ServiceBusLinkProcessor.MaxSessionAcceptWait, timeout);
    }

    [Fact]
    public void ClientTimeout_IsHonoured()
    {
        // The SDK sends a uint number of milliseconds (e.g. 4976 for a 5s TryTimeout).
        var timeout = ServiceBusLinkProcessor.ResolveSessionAcceptWait(AttachWithTimeout(4976u));
        Assert.Equal(TimeSpan.FromMilliseconds(4976), timeout);
    }

    [Fact]
    public void ClientTimeout_LongerThanServiceCap_IsCapped()
    {
        var timeout = ServiceBusLinkProcessor.ResolveSessionAcceptWait(AttachWithTimeout(120_000u));
        Assert.Equal(ServiceBusLinkProcessor.MaxSessionAcceptWait, timeout);
    }

    [Theory]
    [InlineData(0u)]
    [InlineData("not-a-number")]
    public void UnusableTimeout_FallsBackToServiceCap(object value)
    {
        var timeout = ServiceBusLinkProcessor.ResolveSessionAcceptWait(AttachWithTimeout(value));
        Assert.Equal(ServiceBusLinkProcessor.MaxSessionAcceptWait, timeout);
    }
}
