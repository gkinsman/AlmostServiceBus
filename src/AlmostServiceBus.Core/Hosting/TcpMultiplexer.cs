using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Hosting;

/// <summary>
/// Listens on a single public port and routes connections to either the AMQP backend
/// or the HTTP backend based on the first byte of the client's request.
///
/// Handles two connection types:
///   1. Plain AMQP (first byte 0x41 'A', start of "AMQP\0\1\0\0") → proxy to AMQP backend
///   2. Plain HTTP (first byte matches a known HTTP verb)         → proxy to HTTP backend
///
/// The emulator operates in MS-emulator-compat mode only — clients connect with
/// <c>UseDevelopmentEmulator=true</c> in their connection string, which tells
/// <c>Azure.Messaging.ServiceBus</c> to use plain AMQP, and tells the admin client
/// to use plain HTTP. No TLS termination, no certificate handling.
/// </summary>
public class TcpMultiplexer
{
    private static readonly ILogger Log = AlmostServiceBus.Core.Amqp.AmqpLog.CreateLogger<TcpMultiplexer>();

    private const byte AmqpByte = 0x41; // 'A' — start of "AMQP\0\1\0\0"

    /// <summary>
    /// Checks if a byte looks like the start of an HTTP request method
    /// (GET, PUT, POST, DELETE, PATCH, HEAD, OPTIONS).
    /// </summary>
    private static bool IsHttpByte(byte b) => b is
        0x47 or // G (GET)
        0x50 or // P (PUT, POST, PATCH)
        0x44 or // D (DELETE)
        0x48 or // H (HEAD)
        0x4F;   // O (OPTIONS)

    private readonly int _listenPort;
    private readonly int _amqpPort;
    private readonly int _httpPort;

    public TcpMultiplexer(int listenPort, int amqpPort, int httpPort)
    {
        _listenPort = listenPort;
        _amqpPort = amqpPort;
        _httpPort = httpPort;
    }

    public async Task StartAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Any, _listenPort);
        listener.Start(512);

        using var reg = ct.Register(() => listener.Stop());

        try
        {
            while (!ct.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(ct);
                _ = HandleConnectionAsync(client, ct);
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        finally
        {
            listener.Stop();
        }
    }

    private async Task HandleConnectionAsync(TcpClient client, CancellationToken ct)
    {
        TcpClient? backend = null;
        try
        {
            // Disable Nagle: this is a relay hop and AMQP/HTTP handshakes are many
            // small round-trip frames. Without NoDelay, Nagle + delayed-ACK adds
            // tens-to-hundreds of ms per round-trip, making first connects slow.
            client.NoDelay = true;

            var stream = client.GetStream();

            var firstByte = new byte[1];
            var read = await stream.ReadAsync(firstByte.AsMemory(0, 1), ct);
            if (read == 0)
            {
                client.Dispose();
                return;
            }

            if (firstByte[0] == AmqpByte)
            {
                backend = await ConnectToBackend(_amqpPort, ct);
                var backendStream = backend.GetStream();
                await backendStream.WriteAsync(firstByte.AsMemory(0, 1), ct);
                await ProxyBidirectional(stream, backendStream, client, backend, ct);
            }
            else if (IsHttpByte(firstByte[0]))
            {
                backend = await ConnectToBackend(_httpPort, ct);
                var backendStream = backend.GetStream();
                await backendStream.WriteAsync(firstByte.AsMemory(0, 1), ct);
                await ProxyBidirectional(stream, backendStream, client, backend, ct);
            }
            else
            {
                client.Dispose();
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // A peer reset/abort is benign here: it's a health probe, port scan, or
            // a client that disconnected mid-handshake. Aspire's TCP readiness probe
            // routinely connects and closes without sending a byte, which surfaces as
            // SocketException 10054. Don't treat these as warnings.
            if (IsBenignDisconnect(ex))
                Log.LogDebug("TcpMultiplexer: peer closed connection ({Message})", ex.Message);
            else
                Log.LogWarning(ex, "TcpMultiplexer: connection error during proxy");
        }
        finally
        {
            client.Dispose();
            backend?.Dispose();
        }
    }

    private static async Task<TcpClient> ConnectToBackend(int port, CancellationToken ct)
    {
        var backend = new TcpClient();
        await backend.ConnectAsync(IPAddress.Loopback, port, ct);
        backend.NoDelay = true; // see NoDelay note in HandleConnectionAsync
        return backend;
    }

    /// <summary>
    /// A connection reset/abort while proxying means the peer went away — a health
    /// probe, port scan, or client disconnecting mid-handshake. These are expected
    /// and not actionable, so they're logged at debug rather than warning.
    /// </summary>
    private static bool IsBenignDisconnect(Exception ex) => ex switch
    {
        SocketException => true,
        IOException { InnerException: SocketException } => true,
        _ => false,
    };

    private static async Task ProxyBidirectional(
        Stream clientStream, NetworkStream backendStream,
        TcpClient client, TcpClient backend, CancellationToken ct)
    {
        // Wrap each direction so that when one side's copy completes (EOF),
        // we immediately signal half-close on the other side's socket.
        // This ensures ContainerHost sees EOF promptly and can respond
        // with its AMQP Close frame instead of waiting for a timeout.
        var clientToBackend = CopyAndSignalAsync(clientStream, backendStream, backend, ct);
        var backendToClient = CopyAndSignalAsync(backendStream, clientStream, client, ct);

        await Task.WhenAny(clientToBackend, backendToClient);

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        try
        {
            await Task.WhenAll(clientToBackend, backendToClient)
                .WaitAsync(timeout.Token);
        }
        catch { }

        try { client.Client.Shutdown(SocketShutdown.Both); } catch { }
        try { backend.Client.Shutdown(SocketShutdown.Both); } catch { }
    }

    /// <summary>
    /// Copies data from source to destination, then signals half-close on the
    /// destination's underlying socket. This propagates EOF through the proxy
    /// so the peer sees the connection close immediately rather than waiting
    /// for an idle timeout.
    /// </summary>
    private static async Task CopyAndSignalAsync(
        Stream source, Stream destination, TcpClient destinationClient, CancellationToken ct)
    {
        try
        {
            await source.CopyToAsync(destination, ct);
            await destination.FlushAsync(ct);
        }
        catch { }

        try { destinationClient.Client.Shutdown(SocketShutdown.Send); } catch { }
    }
}
