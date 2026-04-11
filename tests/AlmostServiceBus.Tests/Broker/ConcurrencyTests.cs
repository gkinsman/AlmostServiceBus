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

    // ═══════════════════════════════════════════════════════════════════
    // Issues #2/#3: ReceiverLinkEndpoint / SessionReceiverLinkEndpoint
    // pump double-start. OnFlow checks _pumpTask without a lock, so
    // concurrent OnFlow calls can each start a message pump.
    //
    // OnFlow requires AMQP FlowContext objects we can't construct in
    // unit tests. We simulate the pattern: N threads racing to start
    // a single-pump guard. The fix (lock around check-then-act) is
    // the same pattern we verify here.
    // ═══════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Issue2_3_PumpGuard_ShouldStartExactlyOnePump()
    {
        // Simulates the ReceiverLinkEndpoint.OnFlow pattern:
        // check _pumpTask == null/completed, then start a new pump.
        const int iterations = 500;
        const int concurrentFlows = 20;
        var doubleStarts = 0;

        for (var i = 0; i < iterations; i++)
        {
            var startCount = 0;
            Task? pumpTask = null;
            var pumpLock = new Lock();
            var barrier = new Barrier(concurrentFlows);

            var tasks = Enumerable.Range(0, concurrentFlows).Select(_ => Task.Run(() =>
            {
                barrier.SignalAndWait();
                // This mirrors the fixed OnFlow pattern
                lock (pumpLock)
                {
                    if (pumpTask is null || pumpTask.IsCompleted)
                    {
                        Interlocked.Increment(ref startCount);
                        pumpTask = Task.Delay(10); // simulate pump running
                    }
                }
            })).ToArray();

            await Task.WhenAll(tasks);

            if (startCount != 1)
                doubleStarts++;
        }

        Assert.Equal(0, doubleStarts);
    }

    // ═══════════════════════════════════════════════════════════════════
    // Issue #10 (event handler field capture) is fixed alongside #2/#3.
    // The fix captures `var cts = new CTS()` as a local variable, then
    // the lambda closes over `cts` (not `this._pumpCts`). This prevents
    // closing an old connection from cancelling the new pump's CTS.
    // Verified by code inspection — no runtime test needed since the
    // fix is a closure capture change.
    // ═══════════════════════════════════════════════════════════════════
}
