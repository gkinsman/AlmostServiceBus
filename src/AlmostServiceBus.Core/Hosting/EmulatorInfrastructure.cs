using System.Net;
using System.Net.Sockets;

namespace AlmostServiceBus.Core.Hosting;

/// <summary>
/// Shared infrastructure utilities used by both the standalone host and the test fixture.
/// </summary>
public static class EmulatorInfrastructure
{
    /// <summary>
    /// Process-wide lock around port allocation. Prevents the GetFreePort TOCTOU race
    /// (where two threads bind port 0, OS hands them the same port after both close
    /// their probe listener) within a single process. Cross-process races still need to
    /// be handled by the fixture's bind retry loop.
    /// </summary>
    private static readonly Lock s_portLock = new();

    /// <summary>
    /// Tracks ports recently handed out so that back-to-back GetFreePort calls within
    /// the same process don't return the same port (the OS can hand out a freshly-closed
    /// ephemeral port to the very next probe). Uses a small ring buffer; older entries
    /// fall out as the OS recycles ephemeral ports.
    /// </summary>
    private static readonly HashSet<int> s_recentlyAllocated = new();
    private const int MaxRecentlyAllocated = 64;

    /// <summary>
    /// Finds an available TCP port on the loopback interface. Serialized within a process
    /// to avoid the OS handing the same just-freed port to two concurrent callers.
    /// </summary>
    public static int GetFreePort()
    {
        lock (s_portLock)
        {
            for (var attempt = 0; attempt < 16; attempt++)
            {
                var listener = new TcpListener(IPAddress.Loopback, 0);
                listener.Start();
                var port = ((IPEndPoint)listener.LocalEndpoint).Port;
                listener.Stop();

                if (!s_recentlyAllocated.Contains(port))
                {
                    if (s_recentlyAllocated.Count >= MaxRecentlyAllocated)
                    {
                        s_recentlyAllocated.Clear();
                    }
                    s_recentlyAllocated.Add(port);
                    return port;
                }
            }

            var fallback = new TcpListener(IPAddress.Loopback, 0);
            fallback.Start();
            var fallbackPort = ((IPEndPoint)fallback.LocalEndpoint).Port;
            fallback.Stop();
            return fallbackPort;
        }
    }
}
