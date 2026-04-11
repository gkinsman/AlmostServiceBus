using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Hosting;

/// <summary>
/// Listens on a single public port and routes connections to either the AMQP backend
/// or the HTTP backend based on the protocol detected.
///
/// Handles three connection types:
///   1. Plain AMQP (first byte 0x41 'A') → proxy directly to AMQP backend
///   2. TLS with HTTP inside (HTTPS) → terminate TLS, proxy to plain HTTP backend
///   3. TLS with AMQP inside (AMQPS) → terminate TLS, proxy to plain AMQP backend
///
/// TLS termination uses the provided X.509 certificate. After the TLS handshake,
/// the decrypted first byte determines whether the client speaks HTTP or AMQP.
/// </summary>
public class TcpMultiplexer
{
    private static readonly ILogger Log = AlmostServiceBus.Core.Amqp.AmqpLog.CreateLogger<TcpMultiplexer>();

    private const byte AmqpByte = 0x41; // 'A' — start of "AMQP\0\1\0\0"
    private const byte TlsByte = 0x16;  // TLS record type: Handshake

    /// <summary>
    /// Checks if a byte looks like the start of an HTTP request method
    /// (GET, PUT, POST, DELETE, PATCH, HEAD, OPTIONS).
    /// Used for UseDevelopmentEmulator=true which sends plain HTTP without TLS.
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
    private readonly X509Certificate2? _certificate;

    public TcpMultiplexer(int listenPort, int amqpPort, int httpPort, X509Certificate2? certificate = null)
    {
        _listenPort = listenPort;
        _amqpPort = amqpPort;
        _httpPort = httpPort;
        _certificate = certificate;
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
            else if (firstByte[0] == TlsByte && _certificate is not null)
            {
                await HandleTlsConnection(client, stream, firstByte, ct);
            }
            else if (IsHttpByte(firstByte[0]))
            {
                // Plain HTTP (UseDevelopmentEmulator=true skips TLS)
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
            Log.LogWarning(ex, "TcpMultiplexer: connection error during proxy");
        }
        finally
        {
            client.Dispose();
            backend?.Dispose();
        }
    }

    private async Task HandleTlsConnection(TcpClient client, NetworkStream rawStream, byte[] peekedByte, CancellationToken ct)
    {
        var prefixedStream = new PrefixedStream(rawStream, peekedByte);
        var sslStream = new SslStream(prefixedStream, leaveInnerStreamOpen: false);

        try
        {
            await sslStream.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
            {
                ServerCertificate = _certificate,
                ClientCertificateRequired = false,
                ApplicationProtocols = [
                    SslApplicationProtocol.Http2,
                    SslApplicationProtocol.Http11,
                    new SslApplicationProtocol("amqp"),
                ],
            }, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            Log.LogWarning(ex, "TcpMultiplexer: TLS handshake failed on port {Port}", _listenPort);
            sslStream.Dispose();
            return;
        }

        TcpClient? backend = null;
        try
        {
            int backendPort;
            var alpn = sslStream.NegotiatedApplicationProtocol;

            if (alpn == new SslApplicationProtocol("amqp"))
            {
                backendPort = _amqpPort;
            }
            else if (alpn == SslApplicationProtocol.Http2 || alpn == SslApplicationProtocol.Http11)
            {
                backendPort = _httpPort;
            }
            else
            {
                // No ALPN — peek the first decrypted byte to decide
                var innerByte = new byte[1];
                var read = await sslStream.ReadAsync(innerByte.AsMemory(0, 1), ct);
                if (read == 0)
                    return;

                backendPort = innerByte[0] == AmqpByte ? _amqpPort : _httpPort;

                backend = await ConnectToBackend(backendPort, ct);
                var beStream = backend.GetStream();
                await beStream.WriteAsync(innerByte.AsMemory(0, 1), ct);
                await ProxyBidirectional(sslStream, beStream, client, backend, ct);
                return;
            }

            // ALPN matched — proxy the decrypted stream to the backend
            backend = await ConnectToBackend(backendPort, ct);
            var backendStream = backend.GetStream();
            await ProxyBidirectional(sslStream, backendStream, client, backend, ct);
        }
        finally
        {
            backend?.Dispose();
            sslStream.Dispose();
        }
    }

    private static async Task<TcpClient> ConnectToBackend(int port, CancellationToken ct)
    {
        var backend = new TcpClient();
        await backend.ConnectAsync(IPAddress.Loopback, port, ct);
        return backend;
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
            // Source reached EOF — flush any buffered TLS data in the destination
            await destination.FlushAsync(ct);
        }
        catch { }

        // Signal half-close: no more data will be sent on this side
        try { destinationClient.Client.Shutdown(SocketShutdown.Send); } catch { }
    }

    /// <summary>
    /// A stream wrapper that prepends previously-read bytes before the inner stream.
    /// Used to replay the peeked TLS byte so SslStream sees the complete ClientHello.
    /// </summary>
    private sealed class PrefixedStream : Stream
    {
        private readonly Stream _inner;
        private readonly byte[] _prefix;
        private int _prefixOffset;

        public PrefixedStream(Stream inner, byte[] prefix)
        {
            _inner = inner;
            _prefix = prefix;
            _prefixOffset = 0;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => throw new NotSupportedException();
        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (_prefixOffset < _prefix.Length)
            {
                var available = _prefix.Length - _prefixOffset;
                var toCopy = Math.Min(available, count);
                Buffer.BlockCopy(_prefix, _prefixOffset, buffer, offset, toCopy);
                _prefixOffset += toCopy;
                return toCopy;
            }
            return _inner.Read(buffer, offset, count);
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken ct)
        {
            if (_prefixOffset < _prefix.Length)
            {
                var available = _prefix.Length - _prefixOffset;
                var toCopy = Math.Min(available, count);
                Buffer.BlockCopy(_prefix, _prefixOffset, buffer, offset, toCopy);
                _prefixOffset += toCopy;
                return toCopy;
            }
            return await _inner.ReadAsync(buffer, offset, count, ct);
        }

        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken ct = default)
        {
            if (_prefixOffset < _prefix.Length)
            {
                var available = _prefix.Length - _prefixOffset;
                var toCopy = Math.Min(available, buffer.Length);
                _prefix.AsMemory(_prefixOffset, toCopy).CopyTo(buffer);
                _prefixOffset += toCopy;
                return toCopy;
            }
            return await _inner.ReadAsync(buffer, ct);
        }

        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) => _inner.WriteAsync(buffer, offset, count, ct);
        public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => _inner.WriteAsync(buffer, ct);
        public override void Flush() => _inner.Flush();
        public override Task FlushAsync(CancellationToken ct) => _inner.FlushAsync(ct);
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing) _inner.Dispose();
            base.Dispose(disposing);
        }
    }
}
