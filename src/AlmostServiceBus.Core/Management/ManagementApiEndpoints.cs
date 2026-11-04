using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Broker.Sql;
using AlmostServiceBus.Core.Hosting;

namespace AlmostServiceBus.Core.Management;

/// <summary>
/// Maps Service Bus REST management API endpoints onto an <see cref="IEndpointRouteBuilder"/>.
/// </summary>
public static class ManagementApiEndpoints
{
    private const string AtomXmlContentType = "application/atom+xml;type=entry;charset=utf-8";

    public static IEndpointRouteBuilder MapServiceBusManagementApi(
        this IEndpointRouteBuilder app,
        NamespaceRegistry registry,
        ScheduledMessageProcessor? scheduledProcessor = null)
    {
        // Scheduled messages live in the processor, not on the entities, so the runtime
        // ScheduledMessageCount has to be looked up there per namespace.
        Func<string, int>? ScheduledCountFor(NamespaceContext ns) =>
            scheduledProcessor is null ? null : entity => scheduledProcessor.CountScheduledForEntity(ns.Name, entity);

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
            var baseUrl = $"{request.Scheme}://{request.Host}";
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

                // Without If-Match this is a create, and Azure refuses to overwrite an existing rule.
                if (!isUpdate && sub.GetRule(ruleName) is not null)
                    return ManagementApiErrors.Conflict(
                        $"The messaging entity '{ns.Name}:Topic:{topicName}|{subName}|{ruleName}' already exists.");

                var rule = ToRuleEntity(props, ruleName);
                try
                {
                    sub.AddOrUpdateRule(rule);
                }
                catch (SqlSyntaxException ex)
                {
                    return ManagementApiErrors.BadRequest(ex.Message);
                }

                var xml = AtomXmlWriter.WriteRuleEntry(rule, topicName, subName, baseUrl);
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }

            if (TryParseSubscriptionPath(path, out topicName, out subName))
            {
                // PUT /{topicName}/Subscriptions/{subName}
                var topic = ns.GetTopic(topicName);
                if (topic is null)
                    return ManagementApiErrors.EntityNotFound(topicName);

                if (MissingForwardTarget(ns, () =>
                {
                    var p = AtomXmlReader.ReadSubscriptionProperties(body);
                    return [p.ForwardTo, p.ForwardDeadLetteredMessagesTo];
                }) is { } badSub)
                    return badSub;

                if (InvalidDefaultRule(body) is { } badRule)
                    return badRule;

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

                ApplySubscriptionProperties(sub, body);

                var xml = AtomXmlWriter.WriteSubscriptionEntry(sub, baseUrl);
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

                var xml = AtomXmlWriter.WriteTopicEntry(entity, baseUrl, ScheduledCountFor(ns));
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }
            else
            {
                if (MissingForwardTarget(ns, () =>
                {
                    var p = AtomXmlReader.ReadQueueProperties(body);
                    return [p.ForwardTo, p.ForwardDeadLetteredMessagesTo];
                }) is { } badQueue)
                    return badQueue;

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

                var xml = AtomXmlWriter.WriteQueueEntry(entity, baseUrl, ScheduledCountFor(ns));
                return Results.Content(xml, AtomXmlContentType,
                    statusCode: isUpdate ? StatusCodes.Status200OK : StatusCodes.Status201Created);
            }
        });

        app.MapGet("/{**path}", (HttpRequest request) =>
        {
            var baseUrl = $"{request.Scheme}://{request.Host}";
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
                var feed = AtomXmlWriter.WriteQueueFeed(queues, baseUrl, ScheduledCountFor(ns));
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
                var feed = AtomXmlWriter.WriteTopicFeed(topics, baseUrl, ScheduledCountFor(ns));
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

                return Results.Content(AtomXmlWriter.WriteRuleEntry(rule, topicName, subName, baseUrl), AtomXmlContentType);
            }

            if (TryParseRuleListPath(path, out topicName, out subName))
            {
                // GET /{topicName}/Subscriptions/{subName}/Rules?$skip=0&$top=100
                var sub = ns.GetSubscription(topicName, subName);
                if (sub is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

                var (rSkip, rTop) = ParsePagination(request);
                var rules = sub.GetRules().Skip(rSkip).Take(rTop);
                var feed = AtomXmlWriter.WriteRuleFeed(rules, topicName, subName, baseUrl);
                return Results.Content(feed, AtomXmlContentType);
            }

            if (TryParseSubscriptionPath(path, out topicName, out subName))
            {
                // GET /{topicName}/Subscriptions/{subName}
                var sub = ns.GetSubscription(topicName, subName);
                if (sub is null)
                    return ManagementApiErrors.EntityNotFound($"{topicName}/Subscriptions/{subName}");

                return Results.Content(AtomXmlWriter.WriteSubscriptionEntry(sub, baseUrl), AtomXmlContentType);
            }

            if (TryParseSubscriptionListPath(path, out topicName))
            {
                // GET /{topicName}/Subscriptions?$skip=0&$top=100
                var topic = ns.GetTopic(topicName);
                if (topic is null)
                    return ManagementApiErrors.EntityNotFound(topicName);

                var (sSkip, sTop) = ParsePagination(request);
                var subs = topic.GetSubscriptions().Skip(sSkip).Take(sTop);
                var feed = AtomXmlWriter.WriteSubscriptionFeed(subs, baseUrl);
                return Results.Content(feed, AtomXmlContentType);
            }

            // GET /{entityName}
            var entityName = path;

            var queue = ns.GetQueue(entityName);
            if (queue is not null)
                return Results.Content(AtomXmlWriter.WriteQueueEntry(queue, baseUrl, ScheduledCountFor(ns)), AtomXmlContentType);

            var topic2 = ns.GetTopic(entityName);
            if (topic2 is not null)
                return Results.Content(AtomXmlWriter.WriteTopicEntry(topic2, baseUrl, ScheduledCountFor(ns)), AtomXmlContentType);

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
        // Some SDK admin clients append a trailing slash to collection URLs (e.g. ".../Rules/");
        // trim it so the path parsers match. Entity names never legitimately end in '/'.
        return (request.RouteValues["path"]?.ToString() ?? string.Empty).TrimEnd('/');
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
        var namespaceName = EmulatorNetwork.IsDefaultNamespaceHost(host)
            ? "default"
            : host.Split('.')[0];
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
            entity.EnablePartitioning = props.EnablePartitioning;
            entity.EnableExpress = props.EnableExpress;
            entity.EnableBatchedOperations = props.EnableBatchedOperations;
            entity.ForwardTo = props.ForwardTo;
            entity.ForwardDeadLetteredMessagesTo = props.ForwardDeadLetteredMessagesTo;
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
            entity.EnablePartitioning = props.EnablePartitioning;
            entity.EnableExpress = props.EnableExpress;
            entity.EnableBatchedOperations = props.EnableBatchedOperations;
            entity.EnableSubscriptionPartitioning = props.EnableSubscriptionPartitioning;
            entity.SupportOrdering = props.SupportOrdering;
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

    // Real Service Bus refuses to create or update an entity whose ForwardTo names nothing,
    // and the admin client surfaces the 400 as ArgumentException.
    // The same goes for ForwardDeadLetteredMessagesTo.
    private static IResult? MissingForwardTarget(NamespaceContext ns, Func<string?[]> readForwardTargets)
    {
        string?[] targets;
        try { targets = readForwardTargets(); }
        catch { return null; }

        foreach (var forwardTo in targets)
        {
            if (forwardTo is not null && ns.ResolveSendTarget(forwardTo) is (null, null))
                return ManagementApiErrors.BadRequest($"The auto forwarding destination entity {forwardTo} does not exist.");
        }

        return null;
    }

    private static RuleEntity ToRuleEntity(RuleProperties props, string name) => new()
    {
        Name = name,
        FilterType = props.FilterType,
        SqlExpression = props.SqlExpression,
        FilterParameters = props.FilterParameters,
        CorrelationId = props.CorrelationId,
        Subject = props.Subject,
        To = props.To,
        ReplyTo = props.ReplyTo,
        SessionId = props.SessionId,
        ContentType = props.ContentType,
        CorrelationFilterProperties = props.CorrelationFilterProperties,
        ActionExpression = props.ActionExpression,
        ActionParameters = props.ActionParameters,
    };

    // Azure validates an inline DefaultRuleDescription before creating anything, answering 400 (the admin client's
    // ArgumentException) with the SQL error text, and no subscription is created.
    private static IResult? InvalidDefaultRule(string body)
    {
        RuleProperties? rule;
        try { rule = AtomXmlReader.ReadSubscriptionProperties(body).DefaultRule; }
        catch { return null; }

        if (rule is null)
            return null;

        try
        {
            ToRuleEntity(rule, rule.Name).Validate();
            return null;
        }
        catch (SqlSyntaxException ex)
        {
            return ManagementApiErrors.BadRequest(ex.Message);
        }
    }

    private static void ApplySubscriptionProperties(SubscriptionEntity entity, string body)
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
            entity.DeadLetteringOnFilterEvaluationExceptions = props.DeadLetteringOnFilterEvaluationExceptions;
            entity.AutoDeleteOnIdle = props.AutoDeleteOnIdle;

            entity.ForwardTo = props.ForwardTo;
            entity.ForwardDeadLetteredMessagesTo = props.ForwardDeadLetteredMessagesTo;

            // If the request embeds a DefaultRuleDescription, replace the $Default rule
            // with the specified rule.  This is how the Azure SDK implements
            // CreateSubscriptionAsync(options, CreateRuleOptions) — it sends a single PUT
            // with the desired rule inlined rather than making separate DELETE + PUT calls.
            if (props.DefaultRule is not null)
            {
                entity.RemoveRule("$Default");
                var rule = ToRuleEntity(props.DefaultRule, props.DefaultRule.Name);
                entity.AddOrUpdateRule(rule);
            }
        }
        catch
        {
            // Malformed XML — leave defaults
        }
    }
}
