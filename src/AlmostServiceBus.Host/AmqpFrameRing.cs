using System.Text;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Host;

/// <summary>
/// Bounded in-memory buffer of the most recent AMQPNetLite frame-trace lines.
/// Enabled with <c>TRACE_AMQP=ring</c>: frames are recorded continuously at negligible
/// cost and only written to disk when something goes wrong (a connection closed with an
/// error), so a long soak run captures the exact protocol exchange leading up to each
/// failure without producing gigabytes of trace.
/// </summary>
public sealed class AmqpFrameRing
{
    private readonly Queue<string> _lines;
    private readonly int _capacity;
    private readonly string _directory;
    private readonly object _lock = new();
    private DateTime _lastDumpUtc = DateTime.MinValue;

    public AmqpFrameRing(int capacity, string directory)
    {
        _capacity = capacity;
        _lines = new Queue<string>(capacity);
        _directory = directory;
        Directory.CreateDirectory(directory);
    }

    public void Record(Amqp.TraceLevel level, string format, params object[]? args)
    {
        string line;
        try
        {
            line = args is { Length: > 0 } ? string.Format(format, args) : format;
        }
        catch
        {
            line = format;
        }

        var stamped = $"{DateTime.UtcNow:HH:mm:ss.fff} T{Environment.CurrentManagedThreadId,-3} [{level}] {line}";
        lock (_lock)
        {
            if (_lines.Count >= _capacity)
                _lines.Dequeue();
            _lines.Enqueue(stamped);
        }
    }

    /// <summary>
    /// Writes the buffered frames to a timestamped file. Dumps are rate-limited so a
    /// burst of related failures produces one file rather than dozens.
    /// </summary>
    public string? Dump(string reason)
    {
        string[] snapshot;
        lock (_lock)
        {
            var now = DateTime.UtcNow;
            if (now - _lastDumpUtc < TimeSpan.FromSeconds(2))
                return null;
            _lastDumpUtc = now;
            snapshot = _lines.ToArray();
        }

        var path = Path.Combine(_directory, $"amqp-frames-{DateTime.UtcNow:yyyyMMdd-HHmmss-fff}.log");
        var sb = new StringBuilder();
        sb.AppendLine($"# {DateTime.UtcNow:O} — {reason}");
        sb.AppendLine($"# last {snapshot.Length} AMQP frames (oldest first)");
        foreach (var line in snapshot)
            sb.AppendLine(line);
        File.WriteAllText(path, sb.ToString());
        return path;
    }
}

/// <summary>
/// Logger provider that watches the emulator's own warnings and dumps the frame ring
/// whenever an AMQP connection is closed with an error.
/// </summary>
public sealed class FrameDumpOnConnectionErrorProvider(AmqpFrameRing ring) : ILoggerProvider
{
    public ILogger CreateLogger(string categoryName) => new Watcher(ring);
    public void Dispose() { }

    private sealed class Watcher(AmqpFrameRing ring) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel)) return;
            var message = formatter(state, exception);
            if (!message.Contains("AMQP connection closed with error", StringComparison.Ordinal))
                return;

            try
            {
                var path = ring.Dump(message);
                if (path is not null)
                    Console.Error.WriteLine($"[TRACE] frame ring dumped to {path}");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[TRACE] frame ring dump failed: {ex.Message}");
            }
        }
    }
}
