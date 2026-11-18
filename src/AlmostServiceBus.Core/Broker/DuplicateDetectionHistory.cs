using System.Collections.Concurrent;

namespace AlmostServiceBus.Core.Broker;

internal sealed class DuplicateDetectionHistory
{
    private readonly ConcurrentDictionary<string, DateTimeOffset> _acceptedAtByMessageId = new();

    public bool TryRecord(string? messageId, TimeSpan window, DateTimeOffset now)
    {
        if (string.IsNullOrEmpty(messageId))
            return true;

        var cutoff = now - window;
        foreach (var entry in _acceptedAtByMessageId)
        {
            if (entry.Value < cutoff)
                _acceptedAtByMessageId.TryRemove(entry);
        }
        return _acceptedAtByMessageId.TryAdd(messageId, now);
    }
}
