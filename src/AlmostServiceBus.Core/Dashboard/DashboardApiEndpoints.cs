using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.HttpResults;
using Microsoft.AspNetCore.Routing;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Core.Dashboard;

/// <summary>
/// JSON API behind the Vue dashboard. One route per operation:
/// <code>
/// GET    /api/dashboard/info
/// GET    /api/dashboard/namespaces
/// GET    /api/dashboard/namespaces/{ns}/entities
/// GET    /api/dashboard/namespaces/{ns}/queues/{queue}/messages
/// GET    /api/dashboard/namespaces/{ns}/queues/{queue}/deadletter
/// GET    /api/dashboard/namespaces/{ns}/queues/{queue}/properties
/// DELETE /api/dashboard/namespaces/{ns}/queues/{queue}/messages
/// DELETE /api/dashboard/namespaces/{ns}/queues/{queue}/deadletter
/// GET    /api/dashboard/namespaces/{ns}/topics/{topic}/messages
/// GET    /api/dashboard/namespaces/{ns}/topics/{topic}/subscriptions/{subscription}/messages
/// GET    /api/dashboard/namespaces/{ns}/topics/{topic}/subscriptions/{subscription}/deadletter
/// DELETE /api/dashboard/namespaces/{ns}/topics/{topic}/subscriptions/{subscription}/messages
/// DELETE /api/dashboard/namespaces/{ns}/topics/{topic}/subscriptions/{subscription}/deadletter
/// GET    /api/dashboard/diagnostics
/// </code>
/// Entity names may contain slashes (MassTransit and Wolverine both produce hierarchical
/// names), so they travel percent-encoded as a single path segment; see <see cref="EntityName"/>.
/// </summary>
public static class DashboardApiEndpoints
{
    private const int PeekLimit = 50;

    public static IEndpointRouteBuilder MapDashboardApi(
        this IEndpointRouteBuilder app,
        NamespaceRegistry registry,
        EmulatorInfo info)
    {
        var api = app.MapGroup("/api/dashboard");
        api.MapGet("/info", () => info);
        api.MapGet("/namespaces", ListNamespaces);
        api.MapGet("/diagnostics", Diagnostics);

        var namespaceGroup = api.MapGroup("/namespaces/{ns}");
        namespaceGroup.MapGet("/entities", Entities);

        var queueGroup = namespaceGroup.MapGroup("/queues/{queue}");
        queueGroup.MapGet("/messages", (string ns, EntityName queue) => Peek(FindQueue(ns, queue)));
        queueGroup.MapGet("/deadletter", (string ns, EntityName queue) => Peek(FindQueue(ns, queue)?.DeadLetterQueue));
        queueGroup.MapGet("/properties", QueuePropertiesOf);
        queueGroup.MapDelete("/messages", (string ns, EntityName queue) => Purge(FindQueue(ns, queue)));
        queueGroup.MapDelete("/deadletter", (string ns, EntityName queue) => Purge(FindQueue(ns, queue)?.DeadLetterQueue));

        var topicGroup = namespaceGroup.MapGroup("/topics/{topic}");
        topicGroup.MapGet("/messages", TopicMessages);

        var subscriptionGroup = topicGroup.MapGroup("/subscriptions/{subscription}");
        subscriptionGroup.MapGet("/messages", (string ns, EntityName topic, EntityName subscription) =>
            Peek(FindSubscription(ns, topic, subscription)?.Queue));
        subscriptionGroup.MapGet("/deadletter", (string ns, EntityName topic, EntityName subscription) =>
            Peek(FindSubscription(ns, topic, subscription)?.Queue.DeadLetterQueue));
        subscriptionGroup.MapDelete("/messages", (string ns, EntityName topic, EntityName subscription) =>
            Purge(FindSubscription(ns, topic, subscription)?.Queue));
        subscriptionGroup.MapDelete("/deadletter", (string ns, EntityName topic, EntityName subscription) =>
            Purge(FindSubscription(ns, topic, subscription)?.Queue.DeadLetterQueue));

        return app;

        // ── handlers ─────────────────────────────────────────────────────────

        List<NamespaceInfo> ListNamespaces() =>
            registry.ListNamespaces().Select(name =>
            {
                var ns = registry.Get(name);
                return new NamespaceInfo(
                    name,
                    ns?.GetQueues().Count ?? 0,
                    ns?.GetTopics().Count ?? 0,
                    ns?.LastActivityAt ?? DateTimeOffset.MinValue);
            }).ToList();

        Results<Ok<EntityOverview>, NotFound> Entities(string ns)
        {
            var context = registry.Get(ns);
            if (context is null) return TypedResults.NotFound();

            var queues = context.GetQueues().Select(q => new QueueInfo(
                q.Name, q.MessageCount, q.DeadLetterQueue.MessageCount,
                q.TotalMessageCount, q.ConsumedCount, q.MaxDeliveryCount, q.ForwardTo)).ToList();

            var topics = context.GetTopics().Select(t => new TopicInfo(
                t.Name,
                t.GetSubscriptions().Select(s => new SubscriptionInfo(
                    s.Name, s.ForwardTo,
                    (s.ResolvedForwardToQueue ?? s.Queue).MessageCount,
                    s.Queue.DeadLetterQueue.MessageCount,
                    s.GetRules().Count)).ToList()
            )).ToList();

            return TypedResults.Ok(new EntityOverview(queues, topics));
        }

        Results<Ok<QueueProperties>, NotFound> QueuePropertiesOf(string ns, EntityName queue) =>
            FindQueue(ns, queue) is { } q ? TypedResults.Ok(ToQueueProperties(q)) : TypedResults.NotFound();

        // A topic holds no messages itself; show the newest across all its subscriptions.
        Results<Ok<List<MessageInfo>>, NotFound> TopicMessages(string ns, EntityName topic)
        {
            var entity = registry.Get(ns)?.GetTopic(topic.Value);
            if (entity is null) return TypedResults.NotFound();

            var messages = entity.GetSubscriptions()
                .SelectMany(s => s.Queue.PeekMessages(PeekLimit))
                .OrderByDescending(m => m.SequenceNumber)
                .Take(PeekLimit)
                .Select(ToMessageInfo)
                .ToList();
            return TypedResults.Ok(messages);
        }

        QueueEntity? FindQueue(string ns, EntityName queue) =>
            registry.Get(ns)?.GetQueue(queue.Value);

        SubscriptionEntity? FindSubscription(string ns, EntityName topic, EntityName subscription) =>
            registry.Get(ns)?.GetTopic(topic.Value)?.GetSubscription(subscription.Value);
    }

    private static Results<Ok<List<MessageInfo>>, NotFound> Peek(QueueEntity? queue) =>
        queue is null
            ? TypedResults.NotFound()
            : TypedResults.Ok(queue.PeekMessages(PeekLimit).Select(ToMessageInfo).ToList());

    private static Results<Ok, NotFound> Purge(QueueEntity? queue)
    {
        if (queue is null) return TypedResults.NotFound();
        // Receive-and-delete every active message. Locking alone (the old behaviour) only hid
        // them until the lock expired, after which they came back with a higher delivery count.
        while (queue.TryDequeueImmediate() is { LockToken: { } lockToken }) queue.Complete(lockToken);
        return TypedResults.Ok();
    }

    private static object Diagnostics()
    {
        ThreadPool.GetAvailableThreads(out var workerAvail, out var ioAvail);
        ThreadPool.GetMaxThreads(out var workerMax, out var ioMax);
        ThreadPool.GetMinThreads(out var workerMin, out var ioMin);

        return new
        {
            threadPool = new
            {
                workerThreads = new { available = workerAvail, max = workerMax, min = workerMin, inUse = workerMax - workerAvail },
                ioThreads = new { available = ioAvail, max = ioMax, min = ioMin, inUse = ioMax - ioAvail },
                pendingWorkItems = ThreadPool.PendingWorkItemCount,
                threadCount = ThreadPool.ThreadCount,
            },
            process = new
            {
                workingSetMB = Environment.WorkingSet / (1024 * 1024),
                gcTotalMemoryMB = GC.GetTotalMemory(false) / (1024 * 1024),
                gen0Collections = GC.CollectionCount(0),
                gen1Collections = GC.CollectionCount(1),
                gen2Collections = GC.CollectionCount(2),
            },
        };
    }

    // ── projections ──────────────────────────────────────────────────────────

    private static MessageInfo ToMessageInfo(BrokeredMessage m)
    {
        string? bodyText = null;
        Dictionary<string, object>? scalars = null;
        if (m.Body is { Length: > 0 })
        {
            bodyText = Encoding.UTF8.GetString(m.Body);
            scalars = ExtractScalars(bodyText);
        }

        return new MessageInfo(
            m.MessageId, m.SequenceNumber, m.ContentType,
            m.CorrelationId, m.DeliveryCount, m.EnqueuedTimeUtc,
            m.Subject, m.ApplicationProperties, bodyText, scalars,
            m.State.ToString(),
            m.DeadLetterReason, m.DeadLetterErrorDescription, m.DeadLetterSource);
    }

    internal static QueueProperties ToQueueProperties(QueueEntity q) => new(
        q.Name,
        Duration(q.LockDuration)!,
        q.MaxDeliveryCount,
        q.RequiresSession,
        Duration(q.DefaultMessageTimeToLive),
        q.DeadLetteringOnMessageExpiration,
        q.RequiresDuplicateDetection,
        q.RequiresDuplicateDetection ? Duration(q.DuplicateDetectionHistoryTimeWindow) : null,
        q.EnableBatchedOperations,
        q.EnablePartitioning,
        q.EnableExpress,
        q.MaxSizeInMegabytes,
        q.AutoDeleteOnIdle is { } idle ? Duration(idle) : null,
        q.ForwardTo,
        q.ForwardDeadLetteredMessagesTo,
        q.UserMetadata,
        q.MessageCount,
        q.DeadLetterQueue.MessageCount,
        q.TotalMessageCount,
        q.ConsumedCount,
        q.Sessions?.GetSessionIds().Count ?? 0);

    /// <summary>ISO 8601 duration, or null for "unbounded" (TimeSpan.MaxValue).</summary>
    private static string? Duration(TimeSpan ts) =>
        ts == TimeSpan.MaxValue ? null : System.Xml.XmlConvert.ToString(ts);

    private static Dictionary<string, object>? ExtractScalars(string json)
    {
        try
        {
            var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.TryGetProperty("message", out var inner)) root = inner;
            var result = new Dictionary<string, object>();
            foreach (var prop in root.EnumerateObject())
            {
                if (prop.Value.ValueKind is JsonValueKind.String)
                    result[prop.Name] = prop.Value.GetString()!;
                else if (prop.Value.ValueKind is JsonValueKind.Number)
                    result[prop.Name] = prop.Value.GetDouble();
                else if (prop.Value.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    result[prop.Name] = prop.Value.GetBoolean();
                if (result.Count >= 5) break;
            }
            return result.Count > 0 ? result : null;
        }
        catch { return null; }
    }
}

/// <summary>
/// A dashboard route parameter holding a queue, topic or subscription name.
/// </summary>
/// <remarks>
/// Service Bus names can contain <c>/</c>. The dashboard sends them percent-encoded so a whole
/// name occupies one path segment, which is what lets each operation be its own route instead
/// of a catch-all that inspects the tail of the path. Kestrel deliberately leaves <c>%2F</c>
/// encoded (decoding it would change the segment structure of the path) and routing passes it
/// through untouched, so this is the one place it is turned back into a slash. Service Bus names
/// cannot contain <c>%</c>, so the replacement is unambiguous.
/// </remarks>
internal readonly record struct EntityName(string Value)
{
    public static bool TryParse(string? text, out EntityName result)
    {
        result = new EntityName((text ?? string.Empty).Replace("%2F", "/", StringComparison.OrdinalIgnoreCase));
        return text is { Length: > 0 };
    }

    public override string ToString() => Value;
}