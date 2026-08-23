using System.Xml;
using System.Xml.Linq;
using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Core.Management;

/// <summary>
/// Serializes Service Bus entities into Atom XML format compatible with the Azure SDK.
/// </summary>
public static class AtomXmlWriter
{
    private static readonly XNamespace Atom = "http://www.w3.org/2005/Atom";
    private static readonly XNamespace Sb = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    private static readonly XNamespace Xsi = "http://www.w3.org/2001/XMLSchema-instance";

    /// <summary>
    /// Formats a <see cref="TimeSpan"/> in ISO 8601 duration format.
    /// For <see cref="TimeSpan.MaxValue"/>, returns the Azure-specific representation.
    /// </summary>
    public static string FormatTimeSpan(TimeSpan ts)
    {
        if (ts == TimeSpan.MaxValue)
            return "P10675199DT2H48M5.4775807S";
        return XmlConvert.ToString(ts);
    }

    /// <summary>Parses a TimeSpan from an ISO 8601 duration string.</summary>
    private static TimeSpan ParseTimeSpan(string value)
    {
        if (value == "P10675199DT2H48M5.4775807S")
            return TimeSpan.MaxValue;
        return XmlConvert.ToTimeSpan(value);
    }

    // ── Queue ────────────────────────────────────────────────────────────────

    public static string WriteQueueEntry(QueueEntity queue, string baseUrl) =>
        SerializeToString(BuildQueueEntry(queue, baseUrl));

    public static string WriteQueueFeed(IEnumerable<QueueEntity> queues, string baseUrl) =>
        SerializeToString(BuildFeed(queues.Select(q => BuildQueueEntry(q, baseUrl))));

    private static XElement BuildQueueEntry(QueueEntity queue, string baseUrl)
    {
        var desc = new XElement(Sb + "QueueDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi.NamespaceName),
            Elem("LockDuration", FormatTimeSpan(queue.LockDuration)),
            Elem("MaxSizeInMegabytes", queue.MaxSizeInMegabytes),
            Elem("RequiresSession", queue.RequiresSession),
            Elem("DefaultMessageTimeToLive", FormatTimeSpan(queue.DefaultMessageTimeToLive)),
            Elem("DeadLetteringOnMessageExpiration", queue.DeadLetteringOnMessageExpiration),
            Elem("MaxDeliveryCount", queue.MaxDeliveryCount),
            Elem("EnablePartitioning", queue.EnablePartitioning),
            Elem("EnableExpress", queue.EnableExpress),
            Elem("EnableBatchedOperations", queue.EnableBatchedOperations),
            OptElem("ForwardTo", queue.ForwardTo),
            OptElem("UserMetadata", queue.UserMetadata),
            queue.AutoDeleteOnIdle.HasValue ? Elem("AutoDeleteOnIdle", FormatTimeSpan(queue.AutoDeleteOnIdle.Value)) : null,
            queue.RequiresDuplicateDetection ? Elem("RequiresDuplicateDetection", true) : null,
            queue.RequiresDuplicateDetection ? Elem("DuplicateDetectionHistoryTimeWindow", FormatTimeSpan(queue.DuplicateDetectionHistoryTimeWindow)) : null);

        return BuildEntry(queue.Name, desc, baseUrl);
    }

    // ── Topic ────────────────────────────────────────────────────────────────

    public static string WriteTopicEntry(TopicEntity topic, string baseUrl) =>
        SerializeToString(BuildTopicEntry(topic, baseUrl));

    public static string WriteTopicFeed(IEnumerable<TopicEntity> topics, string baseUrl) =>
        SerializeToString(BuildFeed(topics.Select(t => BuildTopicEntry(t, baseUrl))));

    private static XElement BuildTopicEntry(TopicEntity topic, string baseUrl)
    {
        var desc = new XElement(Sb + "TopicDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi.NamespaceName),
            Elem("DefaultMessageTimeToLive", FormatTimeSpan(topic.DefaultMessageTimeToLive)),
            Elem("MaxSizeInMegabytes", topic.MaxSizeInMegabytes),
            Elem("EnablePartitioning", topic.EnablePartitioning),
            Elem("EnableExpress", topic.EnableExpress),
            Elem("EnableBatchedOperations", topic.EnableBatchedOperations),
            Elem("SupportOrdering", topic.SupportOrdering),
            OptElem("UserMetadata", topic.UserMetadata),
            topic.AutoDeleteOnIdle.HasValue ? Elem("AutoDeleteOnIdle", FormatTimeSpan(topic.AutoDeleteOnIdle.Value)) : null,
            topic.RequiresDuplicateDetection ? Elem("RequiresDuplicateDetection", true) : null,
            topic.RequiresDuplicateDetection ? Elem("DuplicateDetectionHistoryTimeWindow", FormatTimeSpan(topic.DuplicateDetectionHistoryTimeWindow)) : null);

        return BuildEntry(topic.Name, desc, baseUrl);
    }

    // ── Subscription ─────────────────────────────────────────────────────────

    public static string WriteSubscriptionEntry(SubscriptionEntity sub, string baseUrl) =>
        SerializeToString(BuildSubscriptionEntry(sub, baseUrl));

    public static string WriteSubscriptionFeed(IEnumerable<SubscriptionEntity> subs, string baseUrl) =>
        SerializeToString(BuildFeed(subs.Select(s => BuildSubscriptionEntry(s, baseUrl))));

    private static XElement BuildSubscriptionEntry(SubscriptionEntity sub, string baseUrl)
    {
        var desc = new XElement(Sb + "SubscriptionDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi.NamespaceName),
            Elem("LockDuration", FormatTimeSpan(sub.LockDuration)),
            Elem("RequiresSession", sub.RequiresSession),
            Elem("DefaultMessageTimeToLive", FormatTimeSpan(sub.DefaultMessageTimeToLive)),
            Elem("DeadLetteringOnMessageExpiration", sub.DeadLetteringOnMessageExpiration),
            Elem("MaxDeliveryCount", sub.MaxDeliveryCount),
            Elem("EnableBatchedOperations", sub.EnableBatchedOperations),
            OptElem("ForwardTo", sub.ForwardTo),
            OptElem("UserMetadata", sub.UserMetadata));

        // Note: For a subscription, the SDK expects the URI to reflect the topic path. 
        // Ensure sub.Name is constructed as "{topicName}/Subscriptions/{subscriptionName}" if needed by the SDK.
        return BuildEntry(sub.Name, desc, baseUrl);
    }

    // ── Rule ─────────────────────────────────────────────────────────────────

    public static string WriteRuleEntry(RuleEntity rule, string baseUrl) =>
        SerializeToString(BuildRuleEntry(rule, baseUrl));

    public static string WriteRuleFeed(IEnumerable<RuleEntity> rules, string baseUrl) =>
        SerializeToString(BuildFeed(rules.Select(r => BuildRuleEntry(r, baseUrl))));

    private static XElement BuildRuleEntry(RuleEntity rule, string baseUrl)
    {
        var filterType = rule.FilterType switch
        {
            FilterType.TrueFilter => "TrueFilter",
            FilterType.FalseFilter => "FalseFilter",
            FilterType.SqlFilter => "SqlFilter",
            FilterType.CorrelationFilter => "CorrelationFilter",
            _ => "TrueFilter"
        };

        // For TrueFilter/FalseFilter the SDK uses a SqlExpression of "1=1"/"1=0" internally,
        // but the type attribute signals the filter kind.
        var filterElement = new XElement(Sb + "Filter",
            new XAttribute(Xsi + "type", filterType));

        if (rule.FilterType is FilterType.SqlFilter or FilterType.TrueFilter)
        {
            var sqlExpr = rule.FilterType == FilterType.TrueFilter ? "1=1" : rule.SqlExpression;
            if (sqlExpr is not null)
                filterElement.Add(new XElement(Sb + "SqlExpression", sqlExpr));
        }
        else if (rule.FilterType == FilterType.CorrelationFilter)
        {
            if (rule.CorrelationId is not null)
                filterElement.Add(new XElement(Sb + "CorrelationId", rule.CorrelationId));
            if (rule.Subject is not null)
                filterElement.Add(new XElement(Sb + "Label", rule.Subject));
            if (rule.To is not null)
                filterElement.Add(new XElement(Sb + "To", rule.To));
            if (rule.ReplyTo is not null)
                filterElement.Add(new XElement(Sb + "ReplyTo", rule.ReplyTo));
            if (rule.SessionId is not null)
                filterElement.Add(new XElement(Sb + "SessionId", rule.SessionId));
            if (rule.ContentType is not null)
                filterElement.Add(new XElement(Sb + "ContentType", rule.ContentType));

            if (rule.CorrelationFilterProperties is { Count: > 0 })
            {
                var propsEl = new XElement(Sb + "Properties");
                foreach (var (key, value) in rule.CorrelationFilterProperties)
                {
                    propsEl.Add(new XElement(Sb + "KeyValueOfstringanyType",
                        new XElement(Sb + "Key", key),
                        new XElement(Sb + "Value", value)));
                }
                filterElement.Add(propsEl);
            }
        }

        var actionType = rule.ActionExpression is null ? "EmptyRuleAction" : "SqlRuleAction";
        var actionElement = new XElement(Sb + "Action",
            new XAttribute(Xsi + "type", actionType));

        if (rule.ActionExpression is not null)
            actionElement.Add(new XElement(Sb + "SqlExpression", rule.ActionExpression));

        var desc = new XElement(Sb + "RuleDescription",
            new XAttribute(XNamespace.Xmlns + "i", Xsi.NamespaceName),
            filterElement,
            actionElement,
            Elem("Name", rule.Name));

        return BuildEntry(rule.Name, desc, baseUrl);
    }

    // ── Shared helpers ───────────────────────────────────────────────────────

    private static XElement BuildEntry(string name, XElement description, string baseUrl) 
    {
        // Strip trailing slashes to prevent "http://hostname.com//topicName"
        baseUrl = baseUrl.TrimEnd('/');
        var resourceUrl = $"{baseUrl}/{name}?api-version=2021-05";

        return new XElement(Atom + "entry",
            new XElement(Atom + "id", resourceUrl),
            new XElement(Atom + "title", new XAttribute("type", "text"), name),
            new XElement(Atom + "author",
                new XElement(Atom + "name", "almost-service-bus")
            ),
            new XElement(Atom + "link", 
                new XAttribute("rel", "self"), 
                new XAttribute("href", resourceUrl)
            ),
            new XElement(Atom + "content", new XAttribute("type", "application/xml"), description)
        );
    }

    private static XElement BuildFeed(IEnumerable<XElement> entries) =>
        new(Atom + "feed",
            new XElement(Atom + "title", "Entities"),
            entries);

    private static XElement Elem(string localName, object value) =>
        new(Sb + localName, value is bool b ? b.ToString().ToLowerInvariant() : value);

    private static XElement? OptElem(string localName, string? value) =>
        value is null ? null : new XElement(Sb + localName, value);

    private static string SerializeToString(XElement element)
    {
        var sw = new StringWriter();
        var settings = new XmlWriterSettings
        {
            OmitXmlDeclaration = true,
            Indent = false,
        };
        using (var writer = XmlWriter.Create(sw, settings))
        {
            element.WriteTo(writer);
        }
        return sw.ToString();
    }
}