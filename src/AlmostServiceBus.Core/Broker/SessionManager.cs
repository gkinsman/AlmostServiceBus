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

    // Lock state is private — mutations only through TryLock/Unlock/RenewLock,
    // which must be called under SessionManager._acceptLock.
    private string? _lockedBy;
    private long _lockedUntilTicks;

    public string SessionId { get; }
    public byte[]? UserState { get; set; }

    public int MessageCount => _messageCount;

    /// <summary>Current lock holder, or null if unlocked. Read-only outside SessionManager.</summary>
    public string? LockedBy => _lockedBy;

    /// <summary>UTC expiry of the current lock. Read-only outside SessionManager.</summary>
    public DateTimeOffset LockedUntil
    {
        get
        {
            var ticks = Interlocked.Read(ref _lockedUntilTicks);
            return ticks == 0 ? default : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    public bool IsLocked => _lockedBy is not null && DateTimeOffset.UtcNow < LockedUntil;

    public SessionState(string sessionId)
    {
        SessionId = sessionId;
    }

    /// <summary>
    /// Attempts to acquire the session lock for the given receiver.
    /// Must be called under <see cref="SessionManager._acceptLock"/>.
    /// Returns true if the lock was acquired (session was unlocked).
    /// </summary>
    internal bool TryLock(string receiverId, TimeSpan duration)
    {
        if (IsLocked)
            return false;

        _lockedBy = receiverId;
        Interlocked.Exchange(ref _lockedUntilTicks, DateTimeOffset.UtcNow.Add(duration).UtcTicks);
        return true;
    }

    /// <summary>
    /// Releases the session lock.
    /// Must be called under <see cref="SessionManager._acceptLock"/>.
    /// </summary>
    internal void Unlock()
    {
        _lockedBy = null;
        Interlocked.Exchange(ref _lockedUntilTicks, 0);
    }

    /// <summary>
    /// Extends the session lock by the given duration from now.
    /// Must be called under <see cref="SessionManager._acceptLock"/>.
    /// Returns the new expiry, or null if the session is not locked.
    /// </summary>
    internal DateTimeOffset? RenewLock(TimeSpan duration)
    {
        if (!IsLocked)
            return null;

        var newExpiry = DateTimeOffset.UtcNow.Add(duration);
        Interlocked.Exchange(ref _lockedUntilTicks, newExpiry.UtcTicks);
        return newExpiry;
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
}

public class SessionManager
{
    private readonly ConcurrentDictionary<string, SessionState> _sessions = new(StringComparer.OrdinalIgnoreCase);
    private readonly TimeSpan _lockDuration;
    private readonly Lock _acceptLock = new();

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
        lock (_acceptLock)
        {
            if (sessionId is not null)
            {
                if (_sessions.TryGetValue(sessionId, out var specific) && specific.TryLock(receiverId, _lockDuration))
                    return specific;
                return null;
            }

            // Next available: find an unlocked session with messages
            foreach (var session in _sessions.Values)
            {
                if (session.MessageCount > 0 && session.TryLock(receiverId, _lockDuration))
                    return session;
            }

            return null;
        }
    }

    /// <summary>
    /// Releases the session lock.
    /// </summary>
    public void ReleaseSession(string sessionId)
    {
        lock (_acceptLock)
        {
            if (_sessions.TryGetValue(sessionId, out var session))
                session.Unlock();
        }
    }

    /// <summary>
    /// Extends the session lock duration.
    /// </summary>
    public DateTimeOffset? RenewSessionLock(string sessionId)
    {
        lock (_acceptLock)
        {
            if (!_sessions.TryGetValue(sessionId, out var session))
                return null;

            return session.RenewLock(_lockDuration);
        }
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

    /// <summary>
    /// Returns whether the given session is currently locked by a receiver.
    /// Used by the lock sweep to skip messages whose session is still active.
    /// </summary>
    public bool IsSessionLocked(string sessionId)
    {
        lock (_acceptLock)
        {
            return _sessions.TryGetValue(sessionId, out var session) && session.IsLocked;
        }
    }

    public IReadOnlyCollection<string> GetAvailableSessionIds()
    {
        lock (_acceptLock)
        {
            return _sessions.Values
                .Where(s => s.MessageCount > 0 && !s.IsLocked)
                .Select(s => s.SessionId)
                .ToList();
        }
    }

    /// <summary>
    /// Returns all known session IDs with their lock status (for diagnostics).
    /// Snapshot is best-effort — lock state may change between read and use.
    /// </summary>
    public IReadOnlyList<string> GetSessionIds()
    {
        lock (_acceptLock)
        {
            return _sessions.Values
                .Select(s => $"{s.SessionId}(locked={s.IsLocked},by={s.LockedBy})")
                .ToList();
        }
    }
}
