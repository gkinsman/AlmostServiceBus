using System.Text;
using System.Text.Json;
using System.Threading.Channels;

namespace OrderFlowDemo.OrderApi.Dashboard;

public static class DashboardSseEndpoint
{
    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    /// <summary>
    /// How long the stream may stay silent before a comment line is sent to keep the
    /// connection (and any proxy in between) alive.
    /// </summary>
    private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(15);

    public static IEndpointRouteBuilder MapDashboardSse(this IEndpointRouteBuilder app)
    {
        app.MapGet("/api/dashboard/events", async (
            DashboardEventBus eventBus,
            HttpContext httpContext,
            CancellationToken ct) =>
        {
            httpContext.Response.Headers.ContentType = "text/event-stream";
            httpContext.Response.Headers.CacheControl = "no-cache";
            httpContext.Response.Headers.Connection = "keep-alive";

            // Flush the headers immediately so EventSource fires onopen without waiting for
            // the first event.
            await httpContext.Response.WriteAsync(": connected\n\n", ct);
            await httpContext.Response.Body.FlushAsync(ct);

            var reader = eventBus.Subscribe();
            var buffer = new StringBuilder();
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    // Wait for the next event, but not forever: a silent stream gets a
                    // keep-alive comment instead.
                    var hasData = await WaitToReadAsync(reader, KeepAliveInterval, ct);
                    if (!hasData)
                    {
                        await httpContext.Response.WriteAsync(": keep-alive\n\n", ct);
                        await httpContext.Response.Body.FlushAsync(ct);
                        continue;
                    }

                    // Drain whatever has accumulated and send it as one write + flush. Under
                    // load (hundreds of transitions a second) one flush per event was a
                    // syscall per event; batching keeps the writer ahead of the publisher so
                    // the bounded channel doesn't have to drop anything.
                    buffer.Clear();
                    while (reader.TryRead(out var evt))
                    {
                        buffer.Append("data: ");
                        buffer.Append(JsonSerializer.Serialize(evt, JsonOpts));
                        buffer.Append("\n\n");
                    }

                    if (buffer.Length > 0)
                    {
                        await httpContext.Response.WriteAsync(buffer.ToString(), ct);
                        await httpContext.Response.Body.FlushAsync(ct);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Client went away.
            }
            finally
            {
                eventBus.Unsubscribe(reader);
            }
        });

        return app;
    }

    /// <summary>
    /// Returns true when the reader has data, false when <paramref name="timeout"/> elapsed
    /// first. Cancellation of <paramref name="ct"/> propagates as an exception.
    /// </summary>
    private static async Task<bool> WaitToReadAsync(ChannelReader<DashboardEvent> reader, TimeSpan timeout, CancellationToken ct)
    {
        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        try
        {
            return await reader.WaitToReadAsync(timeoutCts.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            return false;
        }
    }
}
