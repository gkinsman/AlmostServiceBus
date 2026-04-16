using System.Net;
using System.Net.Sockets;
using System.Text;
using AlmostServiceBus.Core.Hosting;
using AlmostServiceBus.TestHost;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Tests.Hosting;

public class TcpMultiplexerTests : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts = new();

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _cts.Dispose();
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    /// <summary>
    /// Starts a TCP server that echoes back everything it receives, prefixed with a tag.
    /// </summary>
    private Task StartEchoServer(int port, string tag, CancellationToken ct)
    {
        return Task.Run(async () =>
        {
            var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    var client = await listener.AcceptTcpClientAsync(ct);
                    _ = Task.Run(async () =>
                    {
                        using (client)
                        {
                            var stream = client.GetStream();
                            var buffer = new byte[1024];
                            var read = await stream.ReadAsync(buffer, ct);
                            var received = Encoding.UTF8.GetString(buffer, 0, read);
                            var response = Encoding.UTF8.GetBytes($"{tag}:{received}");
                            await stream.WriteAsync(response, ct);
                            client.Client.Shutdown(SocketShutdown.Send);
                        }
                    }, ct);
                }
            }
            finally
            {
                listener.Stop();
            }
        }, ct);
    }

    // Plain AMQP routing (byte 0x41) is tested by the SDK integration tests
    // and MassTransit tests which connect through the multiplexer to the real
    // AMQP server (with SASL). The echo-server approach doesn't work now that
    // the AMQP server requires SASL negotiation.

    [Fact]
    public async Task Closes_Connection_OnUnknownProtocol()
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpPort = GetFreePort();

        _ = StartEchoServer(amqpPort, "AMQP", _cts.Token);
        _ = StartEchoServer(httpPort, "HTTP", _cts.Token);

        var multiplexer = new TcpMultiplexer(publicPort, amqpPort, httpPort);
        _ = multiplexer.StartAsync(_cts.Token);

        await Task.Delay(100);

        using var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        var stream = client.GetStream();

        var payload = new byte[] { 0xFF, 0x01, 0x02 };
        await stream.WriteAsync(payload);

        var buffer = new byte[1024];
        try
        {
            var read = await stream.ReadAsync(buffer);
            Assert.Equal(0, read);
        }
        catch (IOException)
        {
            // Connection reset — also acceptable on Windows
        }
    }

}
