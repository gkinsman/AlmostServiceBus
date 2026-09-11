using System.Net;
using System.Net.Sockets;
using System.Text;
using AlmostServiceBus.Core.Hosting;

namespace AlmostServiceBus.Tests.Hosting;

/// <summary>
/// The multiplexer must not let the server's AMQP protocol header reach the client before the
/// client has sent its own. AMQPNetLite pipelines header + open right after the sasl-outcome;
/// rhea (the Node.js SDK's transport) stops parsing at the sasl-outcome and parks the rest of the
/// chunk until the next socket data event, so a coalesced sasl-outcome + header + open deadlocks
/// the handshake. Reproduced end-to-end by tests/client-sdk-smoke/node/concurrent-connections.mjs.
/// </summary>
public class TcpMultiplexerAmqpHeaderTests : IAsyncDisposable
{
    private static readonly byte[] SaslHeader = [0x41, 0x4D, 0x51, 0x50, 0x03, 0x01, 0x00, 0x00];
    private static readonly byte[] AmqpHeader = [0x41, 0x4D, 0x51, 0x50, 0x00, 0x01, 0x00, 0x00];
    private static readonly byte[] Outcome = Encoding.ASCII.GetBytes("<sasl-outcome>");
    private static readonly byte[] Open = Encoding.ASCII.GetBytes("<open>");

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
    /// A stand-in for AMQPNetLite's listener: on accept it immediately writes the given chunks
    /// (pipelined, without waiting for the client) and records everything it receives.
    /// </summary>
    private static (Task<byte[]> Received, Task Server) StartPipeliningBackend(int port, byte[][] chunksToSend, int expectClientBytes, CancellationToken ct)
    {
        var received = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
        var server = Task.Run(async () =>
        {
            var listener = new TcpListener(IPAddress.Loopback, port);
            listener.Start();
            try
            {
                using var client = await listener.AcceptTcpClientAsync(ct);
                client.NoDelay = true;
                var stream = client.GetStream();
                foreach (var chunk in chunksToSend)
                {
                    await stream.WriteAsync(chunk, ct);
                    await stream.FlushAsync(ct);
                    await Task.Delay(20, ct); // separate writes, like separate SendAsync calls
                }

                var buffer = new byte[1024];
                var total = new MemoryStream();
                while (total.Length < expectClientBytes && !ct.IsCancellationRequested)
                {
                    var n = await stream.ReadAsync(buffer, ct);
                    if (n == 0) break;
                    total.Write(buffer, 0, n);
                }
                received.TrySetResult(total.ToArray());
            }
            catch (Exception ex)
            {
                received.TrySetException(ex);
            }
            finally
            {
                listener.Stop();
            }
        }, ct);
        return (received.Task, server);
    }

    private static async Task<byte[]> ReadAvailableAsync(NetworkStream stream, TimeSpan window)
    {
        var buffer = new byte[4096];
        var total = new MemoryStream();
        using var cts = new CancellationTokenSource(window);
        try
        {
            while (true)
            {
                var n = await stream.ReadAsync(buffer, cts.Token);
                if (n == 0) break;
                total.Write(buffer, 0, n);
            }
        }
        catch (OperationCanceledException) { }
        return total.ToArray();
    }

    private async Task<(TcpClient Client, Task<byte[]> BackendReceived)> ConnectThroughMultiplexerAsync(byte[][] backendChunks, int expectClientBytes = 16)
    {
        var publicPort = GetFreePort();
        var amqpPort = GetFreePort();
        var httpPort = GetFreePort();

        var (backendReceived, _) = StartPipeliningBackend(amqpPort, backendChunks, expectClientBytes, _cts.Token);
        _ = new TcpMultiplexer(publicPort, amqpPort, httpPort).StartAsync(_cts.Token);
        await Task.Delay(100);

        var client = new TcpClient();
        await client.ConnectAsync(IPAddress.Loopback, publicPort);
        client.NoDelay = true;
        return (client, backendReceived);
    }

    [Fact]
    public async Task ServerAmqpHeaderIsWithheldUntilClientSendsItsOwn()
    {
        // Server pipelines outcome + header + open in a single write, as AMQPNetLite does.
        var (client, backendReceived) = await ConnectThroughMultiplexerAsync(
            [[.. Outcome, .. AmqpHeader, .. Open]]);
        using var _ = client;
        var stream = client.GetStream();

        // Client speaks first with its SASL header, like every SDK.
        await stream.WriteAsync(SaslHeader);

        // Only the sasl-outcome may come through; the header and open must wait.
        var early = await ReadAvailableAsync(stream, TimeSpan.FromMilliseconds(400));
        Assert.Equal(Outcome, early);

        // Now the client sends its AMQP header — and the server's header + open are released.
        await stream.WriteAsync(AmqpHeader);
        var late = await ReadAvailableAsync(stream, TimeSpan.FromMilliseconds(400));
        Assert.Equal((byte[])[.. AmqpHeader, .. Open], late);

        // Everything the client sent reached the backend intact and in order.
        Assert.Equal((byte[])[.. SaslHeader, .. AmqpHeader], await backendReceived.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task HeaderSplitAcrossChunksIsStillWithheld()
    {
        // The header arrives from the server in two pieces; the matcher must not leak the first
        // three bytes before deciding whether they begin an AMQP header.
        var (client, _) = await ConnectThroughMultiplexerAsync(
            [Outcome, AmqpHeader[..3], [.. AmqpHeader[3..], .. Open]]);
        using var __ = client;
        var stream = client.GetStream();

        await stream.WriteAsync(SaslHeader);
        var early = await ReadAvailableAsync(stream, TimeSpan.FromMilliseconds(400));
        Assert.Equal(Outcome, early);

        await stream.WriteAsync(AmqpHeader);
        var late = await ReadAvailableAsync(stream, TimeSpan.FromMilliseconds(400));
        Assert.Equal((byte[])[.. AmqpHeader, .. Open], late);
    }

    [Fact]
    public async Task ClientHeaderFirstIsForwardedWithoutDelay()
    {
        // A client that has already sent its AMQP header (no SASL) must not be held up at all.
        var (client, backendReceived) = await ConnectThroughMultiplexerAsync(
            [[.. AmqpHeader, .. Open]], expectClientBytes: 8);
        using var _ = client;
        var stream = client.GetStream();

        await stream.WriteAsync(AmqpHeader);
        var got = await ReadAvailableAsync(stream, TimeSpan.FromMilliseconds(600));
        Assert.Equal((byte[])[.. AmqpHeader, .. Open], got);
        Assert.Equal(AmqpHeader, (await backendReceived.WaitAsync(TimeSpan.FromSeconds(5)))[..8]);
    }
}
