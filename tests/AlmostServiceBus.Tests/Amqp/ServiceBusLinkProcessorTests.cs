using Amqp.Framing;
using Amqp.Types;
using AlmostServiceBus.Core.Amqp;

namespace AlmostServiceBus.Tests.Amqp;

public class ServiceBusLinkProcessorTests
{
    private static readonly Symbol Timeout = new("com.microsoft:timeout");

    [Fact]
    public void ResolveSessionAcceptWait_UsesClientTimeoutProperty()
    {
        // The Azure SDK sends its operation timeout (minus a small buffer) as a uint of milliseconds
        // so that the service gives up before the client does.
        var attach = new Attach { Properties = new Fields { [Timeout] = 59_900u } };

        Assert.Equal(TimeSpan.FromMilliseconds(59_900), ServiceBusLinkProcessor.ResolveSessionAcceptWait(attach));
    }

    [Theory]
    [InlineData(5_000)]
    [InlineData(5_000L)]
    public void ResolveSessionAcceptWait_AcceptsOtherIntegerEncodings(object millis)
    {
        var attach = new Attach { Properties = new Fields { [Timeout] = millis } };

        Assert.Equal(TimeSpan.FromSeconds(5), ServiceBusLinkProcessor.ResolveSessionAcceptWait(attach));
    }

    [Fact]
    public void ResolveSessionAcceptWait_CapsAtServiceMaximum()
    {
        var attach = new Attach { Properties = new Fields { [Timeout] = 600_000u } };

        Assert.Equal(ServiceBusLinkProcessor.MaxSessionAcceptWait, ServiceBusLinkProcessor.ResolveSessionAcceptWait(attach));
    }

    [Fact]
    public void ResolveSessionAcceptWait_FallsBackToServiceMaximum_WhenAbsentOrInvalid()
    {
        Assert.Equal(ServiceBusLinkProcessor.MaxSessionAcceptWait, ServiceBusLinkProcessor.ResolveSessionAcceptWait(new Attach()));
        Assert.Equal(ServiceBusLinkProcessor.MaxSessionAcceptWait,
            ServiceBusLinkProcessor.ResolveSessionAcceptWait(new Attach { Properties = new Fields { [Timeout] = 0u } }));
        Assert.Equal(ServiceBusLinkProcessor.MaxSessionAcceptWait,
            ServiceBusLinkProcessor.ResolveSessionAcceptWait(new Attach { Properties = new Fields { [Timeout] = "soon" } }));
    }
}
