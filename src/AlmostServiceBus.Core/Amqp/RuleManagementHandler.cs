using global::Amqp.Types;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Broker.Sql;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>A $management reply: status, description, optional error condition and body.</summary>
internal sealed record ManagementReply(int StatusCode, string Description, string? ErrorCondition = null, object? Body = null);

/// <summary>
/// The rule operations behind the SDK's <c>ServiceBusRuleManager</c>: <c>com.microsoft:add-rule</c>,
/// <c>remove-rule</c> and <c>enumerate-rules</c> on a subscription's $management node. Wire shapes follow
/// Azure.Messaging.ServiceBus (AmqpRuleManager and its Amqp/Framing codecs); error outcomes follow what Azure
/// returned for the same requests. The AMQP path carries no SQL parameters.
/// </summary>
internal static class RuleManagementHandler
{
    // Described-type codes exactly as the SDK's codecs declare them (the filter codes have one hex digit fewer).
    private const ulong RuleDescriptionCode = 0x0000013700000004;
    private const ulong SqlFilterCode = 0x000001370000006;
    private const ulong TrueFilterCode = 0x000001370000007;
    private const ulong FalseFilterCode = 0x000001370000008;
    private const ulong CorrelationFilterCode = 0x000001370000009;
    private const ulong EmptyRuleActionCode = 0x0000013700000005;
    private const ulong SqlRuleActionCode = 0x0000013700000006;
    private const int CompatibilityLevel = 20;

    // Azure answers an invalid filter or action from the rule manager with this, which the SDK raises as GeneralError.
    private const string InvalidRuleDescription = "The service was unable to process the request; please retry the operation.";

    public static ManagementReply AddRule(SubscriptionEntity? subscription, Map body)
    {
        if (subscription is null)
            return NotFound("The messaging entity could not be found.");

        var name = Get(body, "rule-name") as string;
        if (Get(body, "rule-description") is not Map description || string.IsNullOrEmpty(name))
            return new ManagementReply(400, "A rule-name and rule-description are required.", "com.microsoft:argument-error");

        if (subscription.GetRule(name) is not null)
            return new ManagementReply(409,
                $"The messaging entity '{subscription.TopicName}|{subscription.Name}|{name}' already exists.",
                "com.microsoft:entity-already-exists");

        var rule = new RuleEntity { Name = name };
        if (Get(description, "correlation-filter") is Map correlation)
        {
            rule.FilterType = FilterType.CorrelationFilter;
            rule.CorrelationId = Get(correlation, "correlation-id") as string;
            rule.MessageId = Get(correlation, "message-id") as string;
            rule.To = Get(correlation, "to") as string;
            rule.ReplyTo = Get(correlation, "reply-to") as string;
            rule.Subject = Get(correlation, "label") as string;
            rule.SessionId = Get(correlation, "session-id") as string;
            rule.ReplyToSessionId = Get(correlation, "reply-to-session-id") as string;
            rule.ContentType = Get(correlation, "content-type") as string;
            if (Get(correlation, "properties") is Map properties && properties.Count > 0)
                rule.CorrelationFilterProperties = properties.Keys.ToDictionary(k => k.ToString()!, k => properties[k]);
        }
        else
        {
            // The SDK sends TrueRuleFilter/FalseRuleFilter as sql-filter 1=1 / 1=0, and Azure keeps them as SQL.
            rule.FilterType = FilterType.SqlFilter;
            rule.SqlExpression = Get(description, "sql-filter") is Map sql ? Get(sql, "expression") as string : null;
        }

        if (Get(description, "sql-rule-action") is Map action)
            rule.ActionExpression = Get(action, "expression") as string;

        try
        {
            subscription.AddOrUpdateRule(rule);
        }
        catch (SqlSyntaxException)
        {
            return new ManagementReply(500, InvalidRuleDescription, "amqp:internal-error");
        }

        return new ManagementReply(200, "OK");
    }

    public static ManagementReply RemoveRule(SubscriptionEntity? subscription, Map body)
    {
        var name = Get(body, "rule-name") as string;
        if (subscription is null || name is null || !subscription.RemoveRule(name))
            return NotFound($"The messaging entity '{subscription?.TopicName}|{subscription?.Name}|{name}' could not be found.");
        return new ManagementReply(200, "OK");
    }

    public static ManagementReply EnumerateRules(SubscriptionEntity? subscription, Map body)
    {
        if (subscription is null)
            return NotFound("The messaging entity could not be found.");

        var skip = Convert.ToInt32(Get(body, "skip") ?? 0);
        var top = Convert.ToInt32(Get(body, "top") ?? int.MaxValue);
        var page = subscription.GetRules()
            .OrderBy(r => r.Name, StringComparer.OrdinalIgnoreCase)
            .Skip(skip)
            .Take(top)
            .ToList();
        if (page.Count == 0)
            return new ManagementReply(204, "No Content");

        var rules = new List();
        foreach (var rule in page)
            rules.Add(new Map { ["rule-description"] = Describe(rule) });
        return new ManagementReply(200, "OK", Body: new Map { ["rules"] = rules });
    }

    private static DescribedValue Describe(RuleEntity rule) => new(RuleDescriptionCode, new List
    {
        DescribeFilter(rule),
        rule.ActionExpression is null
            ? new DescribedValue(EmptyRuleActionCode, new List())
            : new DescribedValue(SqlRuleActionCode, new List { rule.ActionExpression, CompatibilityLevel }),
        rule.Name,
        rule.CreatedAt,
    });

    private static DescribedValue DescribeFilter(RuleEntity rule) => rule.FilterType switch
    {
        FilterType.TrueFilter => new DescribedValue(TrueFilterCode, new List()),
        FilterType.FalseFilter => new DescribedValue(FalseFilterCode, new List()),
        FilterType.CorrelationFilter => new DescribedValue(CorrelationFilterCode, new List
        {
            rule.CorrelationId, rule.MessageId, rule.To, rule.ReplyTo, rule.Subject, rule.SessionId,
            rule.ReplyToSessionId, rule.ContentType, ToMap(rule.CorrelationFilterProperties),
        }),
        _ => new DescribedValue(SqlFilterCode, new List { rule.SqlExpression, CompatibilityLevel }),
    };

    private static Map ToMap(Dictionary<string, object>? properties)
    {
        var map = new Map();
        foreach (var (key, value) in properties ?? []) map[key] = value;
        return map;
    }

    private static ManagementReply NotFound(string description) => new(404, description, "amqp:not-found");

    // Map keys arrive as strings from Microsoft.Azure.Amqp and as symbols from other clients.
    private static object? Get(Map map, string key)
    {
        foreach (var k in map.Keys)
        {
            if (k is string s && s == key || k is Symbol sym && (string)sym == key)
                return map[k];
        }

        return null;
    }
}
