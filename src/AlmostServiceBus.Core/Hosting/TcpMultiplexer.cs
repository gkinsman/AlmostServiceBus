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
                await ProxyAmqpBidirectional(stream, backendStream, client, backend, ct);
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

    /// <summary>
    /// The AMQP 1.0 protocol header (<c>AMQP</c>, protocol id 0, version 1.0.0). The SASL
    /// header that precedes it on authenticated connections has protocol id 3 instead.
    /// </summary>
    private static readonly byte[] AmqpProtocolHeader = [0x41, 0x4D, 0x51, 0x50, 0x00, 0x01, 0x00, 0x00];

    /// <summary>
    /// Proxies an AMQP connection, holding the server's AMQP protocol header (and everything
    /// after it) until the client's own AMQP protocol header has been forwarded.
    /// </summary>
    /// <remarks>
    /// AMQPNetLite's listener pipelines its AMQP header and <c>open</c> straight after the
    /// <c>sasl-outcome</c>, without waiting for the client's header. That is legal AMQP, but the
    /// Node.js SDK's transport (rhea) stops parsing at the <c>sasl-outcome</c> frame and parks the
    /// rest of the TCP chunk until the <em>next</em> socket data event. When the three land in one
    /// segment — which happens readily when several connections open at once — that event never
    /// comes: the server has said everything it has to say and is waiting for <c>begin</c>, the
    /// client is waiting for a header it already holds, and the connection hangs until rhea's
    /// ~60 s idle timeout. Releasing the server's header only once the client's header has passed
    /// guarantees it arrives in a later chunk, exactly as it would from a server that waits for the
    /// client's header (as Azure does). The equivalent listener-side change is proposed upstream in
    /// Azure/amqpnetlite#651; doing it here means every distribution of the emulator gets it.
    /// </remarks>
    private static async Task ProxyAmqpBidirectional(
        Stream clientStream, NetworkStream backendStream,
        TcpClient client, TcpClient backend, CancellationToken ct)
    {
        var clientHeaderForwarded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        // The first byte of the client's first header was already consumed and forwarded by the
        // caller, so the client-side matcher starts one byte in.
        var clientToBackend = PumpAsync(
            clientStream, backendStream, backend,
            new HeaderMatcher(AmqpProtocolHeader, initiallyMatched: 1),
            onHeaderComplete: () => clientHeaderForwarded.TrySetResult(),
            releaseAfterHeader: null,
            ct);
        // If the client goes away before ever sending its header, let the other direction drain.
        _ = clientToBackend.ContinueWith(_ => clientHeaderForwarded.TrySetResult(), TaskScheduler.Default);

        var backendToClient = PumpAsync(
            backendStream, clientStream, client,
            new HeaderMatcher(AmqpProtocolHeader, initiallyMatched: 0),
            onHeaderComplete: null,
            releaseAfterHeader: clientHeaderForwarded.Task,
            ct);

        await FinishBidirectional(clientToBackend, backendToClient, client, backend);
    }

    /// <summary>
    /// Copies <paramref name="source"/> to <paramref name="destination"/> while watching for the
    /// AMQP protocol header. In the client→server direction <paramref name="onHeaderComplete"/>
    /// fires once the header has been written through. In the server→client direction the bytes
    /// from the header onward are withheld until <paramref name="releaseAfterHeader"/> completes.
    /// Once the header has been dealt with, the remainder is a plain copy.
    /// </summary>
    private static async Task PumpAsync(
        Stream source, Stream destination, TcpClient destinationClient,
        HeaderMatcher matcher, Action? onHeaderComplete, Task? releaseAfterHeader, CancellationToken ct)
    {
        try
        {
            var buffer = new byte[16 * 1024];
            var headerDone = false;
            // Server→client only: bytes at the end of a chunk that may be the start of the
            // header are withheld until the next chunk decides. Never more than 7 bytes.
            var carry = new List<byte>();

            while (!headerDone)
            {
                var n = await source.ReadAsync(buffer, ct);
                if (n == 0)
                {
                    // EOF before any AMQP header: flush whatever was being held and stop.
                    if (carry.Count > 0)
                        await destination.WriteAsync(carry.ToArray(), ct);
                    return;
                }

                var headerEnd = matcher.Feed(buffer.AsSpan(0, n)); // index just past the header, or -1

                if (releaseAfterHeader is null)
                {
                    // Client→server: forward everything; just note when the header has passed.
                    await destination.WriteAsync(buffer.AsMemory(0, n), ct);
                    if (headerEnd >= 0)
                    {
                        await destination.FlushAsync(ct);
                        headerDone = true;
                        onHeaderComplete?.Invoke();
                    }
                    continue;
                }

                // Server→client: work on carry + chunk so a header straddling chunks is handled.
                var total = new byte[carry.Count + n];
                carry.CopyTo(total, 0);
                buffer.AsSpan(0, n).CopyTo(total.AsSpan(carry.Count));

                if (headerEnd < 0)
                {
                    // Forward all but the trailing partial match, which stays in carry.
                    var keep = matcher.Matched;
                    var forward = total.Length - keep;
                    if (forward > 0)
                    {
                        await destination.WriteAsync(total.AsMemory(0, forward), ct);
                        await destination.FlushAsync(ct);
                    }
                    carry.Clear();
                    carry.AddRange(total.AsSpan(forward));
                    continue;
                }

                headerDone = true;
                var headerStart = carry.Count + headerEnd - AmqpProtocolHeader.Length;

                // Everything before the header (the sasl-outcome) goes now; the header and
                // what follows wait for the client's header to have gone the other way.
                if (headerStart > 0)
                {
                    await destination.WriteAsync(total.AsMemory(0, headerStart), ct);
                    await destination.FlushAsync(ct);
                }
                await releaseAfterHeader.WaitAsync(ct);
                await destination.WriteAsync(total.AsMemory(headerStart), ct);
                await destination.FlushAsync(ct);
            }

            await source.CopyToAsync(destination, ct);
            await destination.FlushAsync(ct);
        }
        catch { }

        try { destinationClient.Client.Shutdown(SocketShutdown.Send); } catch { }
    }

    /// <summary>
    /// Streaming matcher for a fixed byte sequence that may straddle chunk boundaries. The AMQP
    /// header has no repeated prefix, so on a mismatch the only possible restart is at a fresh
    /// first byte.
    /// </summary>
    private sealed class HeaderMatcher
    {
        private readonly byte[] _pattern;
        private int _matched;

        public HeaderMatcher(byte[] pattern, int initiallyMatched)
        {
            _pattern = pattern;
            _matched = initiallyMatched;
        }

        /// <summary>Length of the partial match at the end of the last chunk fed (0 when none).</summary>
        public int Matched => _matched;

        /// <summary>Returns the index just past the completed pattern within <paramref name="chunk"/>, or -1.</summary>
        public int Feed(ReadOnlySpan<byte> chunk)
        {
            for (var i = 0; i < chunk.Length; i++)
            {
                if (chunk[i] == _pattern[_matched])
                {
                    if (++_matched == _pattern.Length)
                        return i + 1;
                }
                else
                {
                    _matched = chunk[i] == _pattern[0] ? 1 : 0;
                }
            }
            return -1;
        }
    }

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

        await FinishBidirectional(clientToBackend, backendToClient, client, backend);
    }

    private static async Task FinishBidirectional(Task clientToBackend, Task backendToClient, TcpClient client, TcpClient backend)
    {
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
