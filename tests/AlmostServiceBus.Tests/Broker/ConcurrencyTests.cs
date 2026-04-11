using System.Collections.Concurrent;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker;

/// <summary>
/// Stress tests that prove concurrency bugs and verify fixes.
/// Each test targets a specific thread-safety issue identified in the review.
/// </summary>
public class ConcurrencyTests
{
    // ═══════════════════════════════════════════════════════════════════
    // Issue #1: SessionManager.TryAcceptSession TOCTOU
    // Two concurrent AcceptNextSession calls can both grab the same session
    // because IsLocked is checked without synchronization.
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Issue1_TryAcceptSession_ShouldNotDoubleAssignSession()
    {
        // A single session with messages. N receivers all race to accept it.
        // Exactly 1 should succeed; the rest must get null.
        const int iterations = 200;
        const int concurrentReceivers = 20;
        var failures = 0;

        for (var i = 0; i < iterations; i++)
        {
            var mgr = new SessionManager(TimeSpan.FromSeconds(30));
            mgr.Enqueue(new BrokeredMessage { SessionId = "session-1", SequenceNumber = 1 });

            // Use a barrier to maximize contention
            var barrier = new Barrier(concurrentReceivers);
            var results = new ConcurrentBag<SessionState>();

            var tasks = Enumerable.Range(0, concurrentReceivers).Select(r => Task.Run(() =>
            {
                barrier.SignalAndWait();
                var session = mgr.TryAcceptSession(null, $"receiver-{r}");
                if (session is not null)
                    results.Add(session);
            })).ToArray();

            await Task.WhenAll(tasks);

            if (results.Count != 1)
                failures++;
        }

        // Before fix: failures should be > 0 (multiple receivers grab same session)
        // After fix: failures must be exactly 0
        Assert.Equal(0, failures);
    }

    [Fact]
    public async Task Issue1_TryAcceptSpecificSession_ShouldNotDoubleAssign()
    {
        // Same race but with a specific session ID rather than next-available.
        const int iterations = 200;
        const int concurrentReceivers = 20;
        var failures = 0;

        for (var i = 0; i < iterations; i++)
        {
            var mgr = new SessionManager(TimeSpan.FromSeconds(30));
            mgr.Enqueue(new BrokeredMessage { SessionId = "target", SequenceNumber = 1 });

            var barrier = new Barrier(concurrentReceivers);
            var results = new ConcurrentBag<SessionState>();

            var tasks = Enumerable.Range(0, concurrentReceivers).Select(r => Task.Run(() =>
            {
                barrier.SignalAndWait();
                var session = mgr.TryAcceptSession("target", $"receiver-{r}");
                if (session is not null)
                    results.Add(session);
            })).ToArray();

            await Task.WhenAll(tasks);

            if (results.Count != 1)
                failures++;
        }

        Assert.Equal(0, failures);
    }
}
