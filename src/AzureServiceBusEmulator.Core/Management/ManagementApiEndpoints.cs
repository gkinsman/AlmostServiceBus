using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Management;

/// <summary>
/// Maps Service Bus REST management API endpoints onto an <see cref="IEndpointRouteBuilder"/>.
/// </summary>
public static class ManagementApiEndpoints
{
    private const string AtomXmlContentType = "application/atom+xml;type=entry;charset=utf-8";

    public static IEndpointRouteBuilder MapServiceBusManagementApi(
        this IEndpointRouteBuilder app,
        NamespaceRegistry registry)
    {
        // MassTransit uses entity names with '/' (e.g. "Namespace/EventType"),
        // so we must use catch-all route parameters and parse the path ourselves
        // to distinguish entity ops from subscription/rule ops.
        //
        // Path patterns:
        //   {entityName}                                          → queue/topic
        //   {topicName}/Subscriptions/{subName}                   → subscription
        //   {topicName}/Subscriptions                             → list subscriptions
        //   {topicName}/Subscriptions/{subName}/Rules/{ruleName}  → rule
        //   {topicName}/Subscriptions/{subName}/Rules             → list rules
        //
        // We split on the literal "/Subscriptions/" and "/Rules/" segments.

        app.MapPut("/{**path}", async (HttpRequest request) =>
        {
            var path = GetRoutePath(request);
            var ns = ResolveNamespace(request, registry);
            var body = await ReadBodyAsync(request);
            var isUpdate = request.Headers.ContainsKey("If-Match");

            if (TryParseRulePath(path, out var topicName, out var subName, out var ruleName))
            {
                // PUT /{topicName}/Subscriptions/{subName}/Rules/{ruleName}
                var sub = ns.GetSubscription(topicName, subName);
                if (sub is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

                RuleProperties props;
                try
                {
                    props = AtomXmlReader.ReadRuleProperties(body);
                }
                catch
                {
                    props = new RuleProperties(ruleName, FilterType.TrueFilter, null, null, null);
                }

                var rule = new RuleEntity
                {
                    Name = ruleName,
                    FilterType = props.FilterType,
                    SqlExpression = props.SqlExpression,
                    CorrelationId = props.CorrelationId,
                    Subject = props.Subject,
                    To = props.To,
                    ReplyTo = props.ReplyTo,
                    SessionId = props.SessionId,
                    ContentType = props.ContentType,
                    CorrelationFilterProperties = props.CorrelationFilterProperties,
                    ActionExpression = props.ActionExpression
                };
                sub.AddOrUpdateRule(rule);

                var xml = AtomXmlWriter.WriteRuleEntry(rule);
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }

            if (TryParseSubscriptionPath(path, out topicName, out subName))
            {
                // PUT /{topicName}/Subscriptions/{subName}
                var topic = ns.GetTopic(topicName);
                if (topic is null)
                    return ManagementApiErrors.EntityNotFound(topicName);

                SubscriptionEntity sub;
                if (isUpdate)
                {
                    var existing = ns.GetSubscription(topicName, subName);
                    if (existing is null)
                        return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");
                    sub = existing;
                }
                else
                {
                    sub = topic.AddSubscription(subName);
                }

                ApplySubscriptionProperties(sub, body, ns);

                var xml = AtomXmlWriter.WriteSubscriptionEntry(sub);
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }

            // PUT /{entityName} — create or update queue/topic
            var entityName = path;
            var isTopic = body.Contains("TopicDescription", StringComparison.OrdinalIgnoreCase);

            if (isTopic)
            {
                TopicEntity entity;
                if (isUpdate)
                {
                    var existing = ns.GetTopic(entityName);
                    if (existing is null)
                        return ManagementApiErrors.EntityNotFound(entityName);
                    entity = existing;
                }
                else
                {
                    entity = ns.CreateTopic(entityName);
                }

                ApplyTopicProperties(entity, body);

                var xml = AtomXmlWriter.WriteTopicEntry(entity);
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }
            else
            {
                QueueEntity entity;
                if (isUpdate)
                {
                    var existing = ns.GetQueue(entityName);
                    if (existing is null)
                        return ManagementApiErrors.EntityNotFound(entityName);
                    entity = existing;
                }
                else
                {
                    entity = ns.CreateQueue(entityName);
                }

                ApplyQueueProperties(entity, body);

                var xml = AtomXmlWriter.WriteQueueEntry(entity);
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }
        });

        app.MapGet("/{**path}", (HttpRequest request) =>
        {
            var path = GetRoutePath(request);
            if (string.IsNullOrEmpty(path))
                return ManagementApiErrors.EntityNotFound("");

            var ns = ResolveNamespace(request, registry);

            // List all queues: GET /$Resources/Queues?$skip=0&$top=100
            if (path.Equals("$Resources/Queues", StringComparison.OrdinalIgnoreCase))
            {
                var (skip, top) = ParsePagination(request);
                var queues = ns.GetQueues()
                    .OrderBy(q => q.Name, StringComparer.OrdinalIgnoreCase)
                    .Skip(skip)
                    .Take(top);
                var feed = AtomXmlWriter.WriteQueueFeed(queues);
                return Results.Content(feed, AtomXmlContentType);
            }

            // List all topics: GET /$Resources/Topics?$skip=0&$top=100
            if (path.Equals("$Resources/Topics", StringComparison.OrdinalIgnoreCase))
            {
                var (skip, top) = ParsePagination(request);
                var topics = ns.GetTopics()
                    .OrderBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
                    .Skip(skip)
                    .Take(top);
                var feed = AtomXmlWriter.WriteTopicFeed(topics);
                return Results.Content(feed, AtomXmlContentType);
            }

            if (TryParseRulePath(path, out var topicName, out var subName, out var ruleName))
            {
                // GET /{topicName}/Subscriptions/{subName}/Rules/{ruleName}
                var sub = ns.GetSubscription(topicName, subName);
                if (sub is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

                var rule = sub.GetRule(ruleName);
                if (rule is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}/Rules/{ruleName}");

                return Results.Content(AtomXmlWriter.WriteRuleEntry(rule), AtomXmlContentType);
            }

            if (TryParseRuleListPath(path, out topicName, out subName))
            {
                // GET /{topicName}/Subscriptions/{subName}/Rules?$skip=0&$top=100
                var sub = ns.GetSubscription(topicName, subName);
                if (sub is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

                var (rSkip, rTop) = ParsePagination(request);
                var rules = sub.GetRules().Skip(rSkip).Take(rTop);
                var feed = AtomXmlWriter.WriteRuleFeed(rules);
                return Results.Content(feed, AtomXmlContentType);
            }

            if (TryParseSubscriptionPath(path, out topicName, out subName))
            {
                // GET /{topicName}/Subscriptions/{subName}
                var sub = ns.GetSubscription(topicName, subName);
                if (sub is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

                return Results.Content(AtomXmlWriter.WriteSubscriptionEntry(sub), AtomXmlContentType);
            }

            if (TryParseSubscriptionListPath(path, out topicName))
            {
                // GET /{topicName}/Subscriptions?$skip=0&$top=100
                var topic = ns.GetTopic(topicName);
                if (topic is null)
                    return ManagementApiErrors.EntityNotFound(topicName);

                var (sSkip, sTop) = ParsePagination(request);
                var subs = topic.GetSubscriptions().Skip(sSkip).Take(sTop);
                var feed = AtomXmlWriter.WriteSubscriptionFeed(subs);
                return Results.Content(feed, AtomXmlContentType);
            }

            // GET /{entityName}
            var entityName = path;

            var queue = ns.GetQueue(entityName);
            if (queue is not null)
                return Results.Content(AtomXmlWriter.WriteQueueEntry(queue), AtomXmlContentType);

            var topic2 = ns.GetTopic(entityName);
            if (topic2 is not null)
                return Results.Content(AtomXmlWriter.WriteTopicEntry(topic2), AtomXmlContentType);

            return ManagementApiErrors.EntityNotFound(entityName);
        });

        app.MapDelete("/{**path}", (HttpRequest request) =>
        {
            var path = GetRoutePath(request);
            var ns = ResolveNamespace(request, registry);

            if (TryParseRulePath(path, out var topicName, out var subName, out var ruleName))
            {
                // DELETE /{topicName}/Subscriptions/{subName}/Rules/{ruleName}
                var sub = ns.GetSubscription(topicName, subName);
                if (sub is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

                if (!sub.RemoveRule(ruleName))
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}/Rules/{ruleName}");

                return Results.Ok();
            }

            if (TryParseSubscriptionPath(path, out topicName, out subName))
            {
                // DELETE /{topicName}/Subscriptions/{subName}
                var topic = ns.GetTopic(topicName);
                if (topic is null || !topic.RemoveSubscription(subName))
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

                return Results.Ok();
            }

            // DELETE /{entityName}
            var entityName = path;

            if (ns.DeleteQueue(entityName) || ns.DeleteTopic(entityName))
                return Results.Ok();

            return ManagementApiErrors.EntityNotFound(entityName);
        });

        return app;
    }

    // ── Path parsing ─────────────────────────────────────────────────────────
    // Entity names can contain '/' (e.g. MassTransit's "Namespace/EventType"),
    // so we split on the literal "/Subscriptions/" and "/Rules/" segments.

    private static string GetRoutePath(HttpRequest request)
    {
        return request.RouteValues["path"]?.ToString() ?? string.Empty;
    }

    /// <summary>
    /// Matches: {topicName}/Subscriptions/{subName}/Rules/{ruleName}
    /// </summary>
    private static bool TryParseRulePath(string path,
        out string topicName, out string subName, out string ruleName)
    {
        topicName = subName = ruleName = string.Empty;
        var subIdx = path.IndexOf("/Subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIdx < 0) return false;

        var afterSub = path[(subIdx + "/Subscriptions/".Length)..];
        var rulesIdx = afterSub.IndexOf("/Rules/", StringComparison.OrdinalIgnoreCase);
        if (rulesIdx < 0) return false;

        topicName = path[..subIdx];
        subName = afterSub[..rulesIdx];
        ruleName = afterSub[(rulesIdx + "/Rules/".Length)..];
        return topicName.Length > 0 && subName.Length > 0 && ruleName.Length > 0;
    }

    /// <summary>
    /// Matches: {topicName}/Subscriptions/{subName}/Rules
    /// </summary>
    private static bool TryParseRuleListPath(string path,
        out string topicName, out string subName)
    {
        topicName = subName = string.Empty;
        var subIdx = path.IndexOf("/Subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIdx < 0) return false;

        var afterSub = path[(subIdx + "/Subscriptions/".Length)..];
        if (!afterSub.EndsWith("/Rules", StringComparison.OrdinalIgnoreCase)) return false;

        topicName = path[..subIdx];
        subName = afterSub[..^"/Rules".Length];
        return topicName.Length > 0 && subName.Length > 0;
    }

    /// <summary>
    /// Matches: {topicName}/Subscriptions/{subName}
    /// (but NOT .../Rules or .../Rules/{ruleName})
    /// </summary>
    private static bool TryParseSubscriptionPath(string path,
        out string topicName, out string subName)
    {
        topicName = subName = string.Empty;
        var subIdx = path.IndexOf("/Subscriptions/", StringComparison.OrdinalIgnoreCase);
        if (subIdx < 0) return false;

        var afterSub = path[(subIdx + "/Subscriptions/".Length)..];
        // Must not contain /Rules
        if (afterSub.Contains("/Rules", StringComparison.OrdinalIgnoreCase)) return false;

        topicName = path[..subIdx];
        subName = afterSub;
        return topicName.Length > 0 && subName.Length > 0;
    }

    /// <summary>
    /// Matches: {topicName}/Subscriptions
    /// </summary>
    private static bool TryParseSubscriptionListPath(string path, out string topicName)
    {
        topicName = string.Empty;
        if (!path.EndsWith("/Subscriptions", StringComparison.OrdinalIgnoreCase)) return false;

        topicName = path[..^"/Subscriptions".Length];
        return topicName.Length > 0;
    }

    // ── Pagination ────────────────────────────────────────────────────────────

    private static (int Skip, int Top) ParsePagination(HttpRequest request)
    {
        var skip = 0;
        var top = 100;

        if (request.Query.TryGetValue("$skip", out var skipValue)
            && int.TryParse(skipValue, out var parsedSkip) && parsedSkip >= 0)
        {
            skip = parsedSkip;
        }

        if (request.Query.TryGetValue("$top", out var topValue)
            && int.TryParse(topValue, out var parsedTop) && parsedTop > 0)
        {
            top = parsedTop;
        }

        return (skip, top);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static NamespaceContext ResolveNamespace(HttpRequest request, NamespaceRegistry registry)
    {
        // Resolve namespace from SharedAccessKeyName in the Authorization header.
        // Connection string: Endpoint=sb://localhost:5672;SharedAccessKeyName={namespace};SharedAccessKey=emulator
        // The SDK sends: Authorization: SharedAccessSignature sr=...&skn={namespace}&...
        var auth = request.Headers.Authorization.FirstOrDefault();
        if (auth is not null)
        {
            var sknIdx = auth.IndexOf("skn=", StringComparison.OrdinalIgnoreCase);
            if (sknIdx >= 0)
            {
                var start = sknIdx + 4;
                var end = auth.IndexOf('&', start);
                var keyName = end >= 0 ? auth[start..end] : auth[start..];
                if (!string.IsNullOrEmpty(keyName)
                    && !keyName.Equals("RootManageSharedAccessKey", StringComparison.OrdinalIgnoreCase))
                {
                    return registry.GetOrCreate(keyName);
                }
            }
        }

        // Fallback: hostname-based resolution (subdomain or localhost → default)
        var host = request.Host.Host ?? string.Empty;
        var namespaceName = host.Split('.')[0];
        if (namespaceName.Equals("localhost", StringComparison.OrdinalIgnoreCase))
            namespaceName = "default";
        return registry.GetOrCreate(namespaceName);
    }

    private static async Task<string> ReadBodyAsync(HttpRequest request)
    {
        request.EnableBuffering();
        using var reader = new System.IO.StreamReader(request.Body, leaveOpen: true);
        var body = await reader.ReadToEndAsync();
        request.Body.Position = 0;
        return body;
    }

    private static void ApplyQueueProperties(QueueEntity entity, string body)
    {
        try
        {
            var props = AtomXmlReader.ReadQueueProperties(body);
            entity.LockDuration = props.LockDuration;
            entity.MaxSizeInMegabytes = props.MaxSizeInMegabytes;
            entity.RequiresSession = props.RequiresSession;
            entity.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
            entity.DeadLetteringOnMessageExpiration = props.DeadLetteringOnMessageExpiration;
            entity.MaxDeliveryCount = props.MaxDeliveryCount;
            entity.EnableBatchedOperations = props.EnableBatchedOperations;
            entity.ForwardTo = props.ForwardTo;
            entity.UserMetadata = props.UserMetadata;
            entity.AutoDeleteOnIdle = props.AutoDeleteOnIdle;
            entity.RequiresDuplicateDetection = props.RequiresDuplicateDetection;
            if (props.DuplicateDetectionHistoryTimeWindow.HasValue)
                entity.DuplicateDetectionHistoryTimeWindow = props.DuplicateDetectionHistoryTimeWindow.Value;
        }
        catch
        {
            // Malformed XML — leave defaults
        }
    }

    private static void ApplyTopicProperties(TopicEntity entity, string body)
    {
        try
        {
            var props = AtomXmlReader.ReadTopicProperties(body);
            entity.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
            entity.MaxSizeInMegabytes = props.MaxSizeInMegabytes;
            entity.EnableBatchedOperations = props.EnableBatchedOperations;
            entity.UserMetadata = props.UserMetadata;
            entity.AutoDeleteOnIdle = props.AutoDeleteOnIdle;
            entity.RequiresDuplicateDetection = props.RequiresDuplicateDetection;
            if (props.DuplicateDetectionHistoryTimeWindow.HasValue)
                entity.DuplicateDetectionHistoryTimeWindow = props.DuplicateDetectionHistoryTimeWindow.Value;
        }
        catch
        {
            // Malformed XML — leave defaults
        }
    }

    private static void ApplySubscriptionProperties(SubscriptionEntity entity, string body, NamespaceContext ns)
    {
        try
        {
            var props = AtomXmlReader.ReadSubscriptionProperties(body);
            entity.LockDuration = props.LockDuration;
            entity.RequiresSession = props.RequiresSession;
            entity.DefaultMessageTimeToLive = props.DefaultMessageTimeToLive;
            entity.DeadLetteringOnMessageExpiration = props.DeadLetteringOnMessageExpiration;
            entity.MaxDeliveryCount = props.MaxDeliveryCount;
            entity.EnableBatchedOperations = props.EnableBatchedOperations;
            entity.UserMetadata = props.UserMetadata;

            if (props.ForwardTo is not null)
            {
                entity.ForwardTo = props.ForwardTo;
                entity.ResolvedForwardToQueue = ns.GetQueue(props.ForwardTo);
            }

            // If the request embeds a DefaultRuleDescription, replace the $Default rule
            // with the specified rule.  This is how the Azure SDK implements
            // CreateSubscriptionAsync(options, CreateRuleOptions) — it sends a single PUT
            // with the desired rule inlined rather than making separate DELETE + PUT calls.
            if (props.DefaultRule is not null)
            {
                entity.RemoveRule("$Default");
                var rule = new RuleEntity
                {
                    Name = props.DefaultRule.Name,
                    FilterType = props.DefaultRule.FilterType,
                    SqlExpression = props.DefaultRule.SqlExpression,
                    CorrelationId = props.DefaultRule.CorrelationId,
                    Subject = props.DefaultRule.Subject,
                    To = props.DefaultRule.To,
                    ReplyTo = props.DefaultRule.ReplyTo,
                    SessionId = props.DefaultRule.SessionId,
                    ContentType = props.DefaultRule.ContentType,
                    CorrelationFilterProperties = props.DefaultRule.CorrelationFilterProperties,
                    ActionExpression = props.DefaultRule.ActionExpression,
                };
                entity.AddOrUpdateRule(rule);
            }
        }
        catch
        {
            // Malformed XML — leave defaults
        }
    }
}
