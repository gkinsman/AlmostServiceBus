using System.Collections.Concurrent;

namespace AlmostServiceBus.Core.Broker;

/// <summary>
/// Holds per-session state including a priority queue that delivers messages
/// in sequence-number order (FIFO), regardless of the order concurrent
/// producers write to it.
/// </summary>
public class SessionState
{
    private readonly PriorityQueue<BrokeredMessage, long> _messages = new();
    private readonly SemaphoreSlim _signal = new(0);
    private readonly object _lock = new();
    private int _messageCount;

    public string SessionId { get; }
    public string? LockedBy { get; set; }
    public DateTimeOffset LockedUntil { get; set; }
    public byte[]? UserState { get; set; }

    public int MessageCount => _messageCount;

    public SessionState(string sessionId)
    {
        SessionId = sessionId;
    }

    /// <summary>
    /// Adds a message to the session, ordered by <see cref="BrokeredMessage.SequenceNumber"/>.
    /// Thread-safe.
    /// </summary>
    public void Enqueue(BrokeredMessage message)
    {
        lock (_lock)
        {
            _messages.Enqueue(message, message.SequenceNumber);
            Interlocked.Increment(ref _messageCount);
        }

        _signal.Release();
    }

    /// <summary>
    /// Attempts to dequeue the message with the lowest sequence number.
    /// Returns <c>false</c> when the queue is empty.
    /// </summary>
    public bool TryDequeue(out BrokeredMessage? message)
    {
        lock (_lock)
        {
            if (_messages.Count > 0)
            {
                message = _messages.Dequeue();
                Interlocked.Decrement(ref _messageCount);
                return true;
            }
        }

        message = null;
        return false;
    }

    /// <summary>
    /// Asynchronously waits until at least one message is available.
    /// </summary>
    public Task WaitToReadAsync(CancellationToken ct) => _signal.WaitAsync(ct);

    public bool IsLocked => LockedBy is not null && DateTimeOffset.UtcNow < LockedUntil;
}

public class SessionManager
{
    private readonly ConcurrentDictionary<string, SessionState> _sessions = new(StringComparer.OrdinalIgnoreCase);
    private readonly TimeSpan _lockDuration;

    public SessionManager(TimeSpan lockDuration)
    {
        _lockDuration = lockDuration;
    }

    /// <summary>
    /// Routes a message to the appropriate session channel by SessionId.
    /// Creates the session lazily if it doesn't exist.
    /// </summary>
    public void Enqueue(BrokeredMessage message)
    {
        if (string.IsNullOrEmpty(message.SessionId))
            throw new InvalidOperationException("Messages sent to a session-enabled queue must have a SessionId.");

        var session = _sessions.GetOrAdd(message.SessionId, id => new SessionState(id));
        session.Enqueue(message);
    }

    /// <summary>
    /// Locks a session for exclusive access by a receiver.
    /// If sessionId is null, picks the next available session (one with messages, not locked).
    /// Returns null if no session is available.
    /// </summary>
    public SessionState? TryAcceptSession(string? sessionId, string receiverId)
    {
        if (sessionId is not null)
        {
            if (_sessions.TryGetValue(sessionId, out var specific) && !specific.IsLocked)
            {
                specific.LockedBy = receiverId;
                specific.LockedUntil = DateTimeOffset.UtcNow.Add(_lockDuration);
                return specific;
            }
            return null;
        }

        // Next available: find an unlocked session with messages
        foreach (var session in _sessions.Values)
        {
            if (!session.IsLocked && session.MessageCount > 0)
            {
                session.LockedBy = receiverId;
                session.LockedUntil = DateTimeOffset.UtcNow.Add(_lockDuration);
                return session;
            }
        }

        return null;
    }

    /// <summary>
    /// Releases the session lock.
    /// </summary>
    public void ReleaseSession(string sessionId)
    {
        if (_sessions.TryGetValue(sessionId, out var session))
        {
            session.LockedBy = null;
            session.LockedUntil = default;
        }
    }

    /// <summary>
    /// Extends the session lock duration.
    /// </summary>
    public DateTimeOffset? RenewSessionLock(string sessionId)
    {
        if (!_sessions.TryGetValue(sessionId, out var session) || !session.IsLocked)
            return null;

        session.LockedUntil = DateTimeOffset.UtcNow.Add(_lockDuration);
        return session.LockedUntil;
    }

    public byte[]? GetSessionState(string sessionId)
    {
        return _sessions.TryGetValue(sessionId, out var session) ? session.UserState : null;
    }

    public void SetSessionState(string sessionId, byte[]? state)
    {
        var session = _sessions.GetOrAdd(sessionId, id => new SessionState(id));
        session.UserState = state;
    }

    public IReadOnlyCollection<string> GetAvailableSessionIds()
    {
        return _sessions.Values
            .Where(s => s.MessageCount > 0 && !s.IsLocked)
            .Select(s => s.SessionId)
            .ToList();
    }

    /// <summary>
    /// Returns all known session IDs with their lock status (for diagnostics).
    /// </summary>
    public IEnumerable<string> GetSessionIds()
    {
        return _sessions.Values.Select(s => $"{s.SessionId}(locked={s.IsLocked},by={s.LockedBy})");
    }
}
