using AlmostServiceBus.Core.Hosting;

namespace AlmostServiceBus.Tests.Hosting;

public class EmulatorNetworkTests
{
    [Theory]
    [InlineData("localhost")]
    [InlineData("LOCALHOST")]
    [InlineData("127.0.0.1")]
    [InlineData("0.0.0.0")]
    [InlineData("::1")]
    [InlineData("host.docker.internal")]
    [InlineData("localhost:5672")]
    [InlineData("localhost.")]
    [InlineData(" localhost ")]
    public void IsDefaultNamespaceHost_ReturnsTrueForDefaultHosts(string host)
    {
        Assert.True(EmulatorNetwork.IsDefaultNamespaceHost(host));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void IsDefaultNamespaceHost_ReturnsTrueForMissingHost(string? host)
    {
        Assert.True(EmulatorNetwork.IsDefaultNamespaceHost(host));
    }

    [Fact]
    public void IsDefaultNamespaceHost_ReturnsTrueForContainerHostname()
    {
        // The container's own hostname must resolve to the default namespace without any config.
        Assert.True(EmulatorNetwork.IsDefaultNamespaceHost(Environment.MachineName));
    }

    [Theory]
    [InlineData("my-namespace")]
    [InlineData("tenant-1.servicebus.windows.net")]
    [InlineData("contoso")]
    [InlineData("servicebus-emulator")]
    public void IsDefaultNamespaceHost_ReturnsFalseForNamespaceHosts(string host)
    {
        Assert.False(EmulatorNetwork.IsDefaultNamespaceHost(host));
    }

    [Fact]
    public void IsDefaultNamespaceHost_HonoursConfiguredPublicHost()
    {
        var original = Environment.GetEnvironmentVariable(EmulatorNetwork.HostEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable(EmulatorNetwork.HostEnvironmentVariable, "servicebus-emulator");

            Assert.True(EmulatorNetwork.IsDefaultNamespaceHost("servicebus-emulator"));
            Assert.True(EmulatorNetwork.IsDefaultNamespaceHost("servicebus-emulator:5672"));
            Assert.False(EmulatorNetwork.IsDefaultNamespaceHost("other-host"));
        }
        finally
        {
            Environment.SetEnvironmentVariable(EmulatorNetwork.HostEnvironmentVariable, original);
        }
    }

    [Fact]
    public void IsDefaultNamespaceHost_HonoursConfiguredAliases()
    {
        var original = Environment.GetEnvironmentVariable("ASB_DEFAULT_NAMESPACE_HOSTS");
        try
        {
            Environment.SetEnvironmentVariable("ASB_DEFAULT_NAMESPACE_HOSTS", "sb-emulator,my-alias");

            Assert.True(EmulatorNetwork.IsDefaultNamespaceHost("sb-emulator"));
            Assert.True(EmulatorNetwork.IsDefaultNamespaceHost("my-alias"));
            Assert.False(EmulatorNetwork.IsDefaultNamespaceHost("other-host"));
        }
        finally
        {
            Environment.SetEnvironmentVariable("ASB_DEFAULT_NAMESPACE_HOSTS", original);
        }
    }

    [Fact]
    public void GetPublicHost_ReturnsConfiguredHost()
    {
        var original = Environment.GetEnvironmentVariable(EmulatorNetwork.HostEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable(EmulatorNetwork.HostEnvironmentVariable, "my-emulator");

            Assert.Equal("my-emulator", EmulatorNetwork.GetPublicHost());
        }
        finally
        {
            Environment.SetEnvironmentVariable(EmulatorNetwork.HostEnvironmentVariable, original);
        }
    }
}
