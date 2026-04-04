using System.Xml;
using System.Xml.Linq;
using AzureServiceBusEmulator.Core.Broker;

namespace AzureServiceBusEmulator.Core.Management;

// ── Property records ─────────────────────────────────────────────────────────

public record QueueProperties(
    TimeSpan LockDuration,
    long MaxSizeInMegabytes,
    bool RequiresSession,
    TimeSpan DefaultMessageTimeToLive,
    bool DeadLetteringOnMessageExpiration,
    int MaxDeliveryCount,
    bool EnableBatchedOperations,
    string? ForwardTo,
    string? UserMetadata,
    TimeSpan? AutoDeleteOnIdle = null,
    bool RequiresDuplicateDetection = false,
    TimeSpan? DuplicateDetectionHistoryTimeWindow = null);

public record TopicProperties(
    TimeSpan DefaultMessageTimeToLive,
    long MaxSizeInMegabytes,
    bool EnableBatchedOperations,
    string? UserMetadata,
    TimeSpan? AutoDeleteOnIdle = null,
    bool RequiresDuplicateDetection = false,
    TimeSpan? DuplicateDetectionHistoryTimeWindow = null);

public record SubscriptionProperties(
    TimeSpan LockDuration,
    bool RequiresSession,
    TimeSpan DefaultMessageTimeToLive,
    bool DeadLetteringOnMessageExpiration,
    int MaxDeliveryCount,
    bool EnableBatchedOperations,
    string? ForwardTo,
    string? UserMetadata,
    RuleProperties? DefaultRule = null);

public record RuleProperties(
    string Name,
    FilterType FilterType,
    string? SqlExpression,
    string? CorrelationId,
    string? ActionExpression,
    string? Subject = null,
    string? To = null,
    string? ReplyTo = null,
    string? SessionId = null,
    string? ContentType = null,
    Dictionary<string, object>? CorrelationFilterProperties = null);

// ── Reader ───────────────────────────────────────────────────────────────────

/// <summary>
/// Deserializes Service Bus entities from Atom XML format as produced by <see cref="AtomXmlWriter"/>
/// or as returned by the real Azure Service Bus management API.
/// </summary>
public static class AtomXmlReader
{
    private static readonly XNamespace Sb = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    private static readonly XNamespace Xsi = "http://www.w3.org/2001/XMLSchema-instance";

    // ── Public API ───────────────────────────────────────────────────────────

    public static QueueProperties ReadQueueProperties(string xml)
    {
        var desc = ParseDescription(xml, Sb + "QueueDescription");
        return new QueueProperties(
            LockDuration: ParseOptionalTimeSpan(desc, "LockDuration") ?? TimeSpan.FromSeconds(30),
            MaxSizeInMegabytes: ParseOptionalLong(desc, "MaxSizeInMegabytes") ?? 1024,
            RequiresSession: ParseOptionalBool(desc, "RequiresSession") ?? false,
            DefaultMessageTimeToLive: ParseOptionalTimeSpan(desc, "DefaultMessageTimeToLive") ?? TimeSpan.MaxValue,
            DeadLetteringOnMessageExpiration: ParseOptionalBool(desc, "DeadLetteringOnMessageExpiration") ?? false,
            MaxDeliveryCount: ParseOptionalInt(desc, "MaxDeliveryCount") ?? 10,
            EnableBatchedOperations: ParseOptionalBool(desc, "EnableBatchedOperations") ?? true,
            ForwardTo: NormalizeForwardTo(ParseOptionalString(desc, "ForwardTo")),
            UserMetadata: ParseOptionalString(desc, "UserMetadata"),
            AutoDeleteOnIdle: ParseOptionalTimeSpan(desc, "AutoDeleteOnIdle"),
            RequiresDuplicateDetection: ParseOptionalBool(desc, "RequiresDuplicateDetection") ?? false,
            DuplicateDetectionHistoryTimeWindow: ParseOptionalTimeSpan(desc, "DuplicateDetectionHistoryTimeWindow"));
    }

    public static TopicProperties ReadTopicProperties(string xml)
    {
        var desc = ParseDescription(xml, Sb + "TopicDescription");
        return new TopicProperties(
            DefaultMessageTimeToLive: ParseOptionalTimeSpan(desc, "DefaultMessageTimeToLive") ?? TimeSpan.MaxValue,
            MaxSizeInMegabytes: ParseOptionalLong(desc, "MaxSizeInMegabytes") ?? 1024,
            EnableBatchedOperations: ParseOptionalBool(desc, "EnableBatchedOperations") ?? true,
            UserMetadata: ParseOptionalString(desc, "UserMetadata"),
            AutoDeleteOnIdle: ParseOptionalTimeSpan(desc, "AutoDeleteOnIdle"),
            RequiresDuplicateDetection: ParseOptionalBool(desc, "RequiresDuplicateDetection") ?? false,
            DuplicateDetectionHistoryTimeWindow: ParseOptionalTimeSpan(desc, "DuplicateDetectionHistoryTimeWindow"));
    }

    public static SubscriptionProperties ReadSubscriptionProperties(string xml)
    {
        var desc = ParseDescription(xml, Sb + "SubscriptionDescription");

        RuleProperties? defaultRule = null;
        var defaultRuleEl = desc.Element(Sb + "DefaultRuleDescription");
        if (defaultRuleEl is not null)
        {
            var ruleName = ParseOptionalString(defaultRuleEl, "Name") ?? "$Default";
            defaultRule = ParseRuleFromElement(ruleName, defaultRuleEl);
        }

        return new SubscriptionProperties(
            LockDuration: ParseOptionalTimeSpan(desc, "LockDuration") ?? TimeSpan.FromSeconds(30),
            RequiresSession: ParseOptionalBool(desc, "RequiresSession") ?? false,
            DefaultMessageTimeToLive: ParseOptionalTimeSpan(desc, "DefaultMessageTimeToLive") ?? TimeSpan.MaxValue,
            DeadLetteringOnMessageExpiration: ParseOptionalBool(desc, "DeadLetteringOnMessageExpiration") ?? false,
            MaxDeliveryCount: ParseOptionalInt(desc, "MaxDeliveryCount") ?? 10,
            EnableBatchedOperations: ParseOptionalBool(desc, "EnableBatchedOperations") ?? true,
            ForwardTo: NormalizeForwardTo(ParseOptionalString(desc, "ForwardTo")),
            UserMetadata: ParseOptionalString(desc, "UserMetadata"),
            DefaultRule: defaultRule);
    }

    public static RuleProperties ReadRuleProperties(string xml)
    {
        var desc = ParseDescription(xml, Sb + "RuleDescription");
        var name = ParseString(desc, "Name");
        return ParseRuleFromElement(name, desc);
    }

    private static RuleProperties ParseRuleFromElement(string name, XElement ruleEl)
    {
        var filterEl = ruleEl.Element(Sb + "Filter");
        if (filterEl is null)
            return new RuleProperties(name, FilterType.TrueFilter, null, null, null);

        var xsiType = filterEl.Attribute(Xsi + "type")?.Value ?? "TrueFilter";

        FilterType filterType = xsiType switch
        {
            "TrueFilter" => FilterType.TrueFilter,
            "FalseFilter" => FilterType.FalseFilter,
            "SqlFilter" => FilterType.SqlFilter,
            "CorrelationFilter" => FilterType.CorrelationFilter,
            _ => FilterType.TrueFilter
        };

        string? sqlExpression = null;
        string? correlationId = null;
        string? subject = null;
        string? to = null;
        string? replyTo = null;
        string? sessionId = null;
        string? contentType = null;
        Dictionary<string, object>? correlationFilterProperties = null;

        if (filterType == FilterType.SqlFilter)
        {
            sqlExpression = ParseOptionalString(filterEl, "SqlExpression");
        }
        else if (filterType == FilterType.CorrelationFilter)
        {
            correlationId = ParseOptionalString(filterEl, "CorrelationId");
            subject = ParseOptionalString(filterEl, "Label");
            to = ParseOptionalString(filterEl, "To");
            replyTo = ParseOptionalString(filterEl, "ReplyTo");
            sessionId = ParseOptionalString(filterEl, "SessionId");
            contentType = ParseOptionalString(filterEl, "ContentType");

            var propsEl = filterEl.Element(Sb + "Properties");
            if (propsEl is not null)
            {
                correlationFilterProperties = new Dictionary<string, object>();
                foreach (var kvp in propsEl.Elements(Sb + "KeyValueOfstringanyType"))
                {
                    var key = kvp.Element(Sb + "Key")?.Value;
                    var value = kvp.Element(Sb + "Value")?.Value;
                    if (key is not null && value is not null)
                        correlationFilterProperties[key] = value;
                }
            }
        }

        var actionEl = ruleEl.Element(Sb + "Action");
        string? actionExpression = null;
        if (actionEl is not null)
        {
            var actionType = actionEl.Attribute(Xsi + "type")?.Value;
            if (actionType == "SqlRuleAction")
                actionExpression = ParseOptionalString(actionEl, "SqlExpression");
        }

        return new RuleProperties(name, filterType, sqlExpression, correlationId, actionExpression,
            subject, to, replyTo, sessionId, contentType, correlationFilterProperties);
    }

    // ── Internal helpers ─────────────────────────────────────────────────────

    private static XElement ParseDescription(string xml, XName descriptionElementName)
    {
        var doc = XDocument.Parse(xml);
        // The description can be inside <content> in an <entry>, or directly as root (for feeds we'd parse entries separately)
        var desc = doc.Descendants(descriptionElementName).FirstOrDefault()
            ?? throw new InvalidOperationException($"Element <{descriptionElementName.LocalName}> not found in XML.");
        return desc;
    }

    private static string ParseString(XElement parent, string localName) =>
        parent.Element(Sb + localName)?.Value
            ?? throw new InvalidOperationException($"Missing required element <{localName}>.");

    private static string? ParseOptionalString(XElement parent, string localName) =>
        parent.Element(Sb + localName)?.Value;

    private static int ParseInt(XElement parent, string localName) =>
        int.Parse(ParseString(parent, localName));

    private static long ParseLong(XElement parent, string localName) =>
        long.Parse(ParseString(parent, localName));

    private static bool ParseBool(XElement parent, string localName) =>
        bool.Parse(ParseString(parent, localName));

    private static TimeSpan ParseTimeSpan(XElement parent, string localName)
    {
        var value = ParseString(parent, localName);
        if (value == "P10675199DT2H48M5.4775807S")
            return TimeSpan.MaxValue;
        return XmlConvert.ToTimeSpan(value);
    }

    private static int? ParseOptionalInt(XElement parent, string localName)
    {
        var value = ParseOptionalString(parent, localName);
        return value is null ? null : int.Parse(value);
    }

    private static long? ParseOptionalLong(XElement parent, string localName)
    {
        var value = ParseOptionalString(parent, localName);
        return value is null ? null : long.Parse(value);
    }

    private static bool? ParseOptionalBool(XElement parent, string localName)
    {
        var value = ParseOptionalString(parent, localName);
        return value is null ? null : bool.Parse(value);
    }

    private static TimeSpan? ParseOptionalTimeSpan(XElement parent, string localName)
    {
        var value = ParseOptionalString(parent, localName);
        if (value is null) return null;
        if (value == "P10675199DT2H48M5.4775807S")
            return TimeSpan.MaxValue;
        return XmlConvert.ToTimeSpan(value);
    }

    /// <summary>
    /// The Azure SDK sends ForwardTo as a full URI (e.g. "sb://ns.servicebus.windows.net/queue-name"
    /// or "http://ns.localhost/queue-name"). This normalizes it to just the entity name.
    /// </summary>
    private static string? NormalizeForwardTo(string? forwardTo)
    {
        if (forwardTo is null) return null;

        // If it looks like a URI, extract the path (entity name)
        if (Uri.TryCreate(forwardTo, UriKind.Absolute, out var uri))
        {
            var path = uri.AbsolutePath.TrimStart('/');
            return string.IsNullOrEmpty(path) ? forwardTo : path;
        }

        return forwardTo;
    }
}
