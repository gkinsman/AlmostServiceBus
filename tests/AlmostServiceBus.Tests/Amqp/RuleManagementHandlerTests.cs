using AlmostServiceBus.Core.Amqp;
using AlmostServiceBus.Core.Broker;
using global::Amqp.Types;

namespace AlmostServiceBus.Tests.Amqp;

/// <summary>
/// The $management rule operations the SDK's ServiceBusRuleManager uses, with the wire shapes from
/// Azure.Messaging.ServiceBus (AmqpRuleManager / Amqp/Framing codecs) and Azure's recorded error outcomes.
/// </summary>
public class RuleManagementHandlerTests
{
    private const ulong RuleDescriptionCode = 0x0000013700000004;
    private const ulong SqlFilterCode = 0x000001370000006;
    private const ulong TrueFilterCode = 0x000001370000007;
    private const ulong CorrelationFilterCode = 0x000001370000009;
    private const ulong EmptyRuleActionCode = 0x0000013700000005;
    private const ulong SqlRuleActionCode = 0x0000013700000006;

    private static SubscriptionEntity Sub() => new("sub", "topic");

    // As AmqpRuleManager.AddRuleInternalAsync builds it (string keys, as Microsoft.Azure.Amqp sends them).
    private static Map AddRuleBody(string name, string? filter = null, string? action = null, Map? correlation = null)
    {
        var description = new Map { ["rule-name"] = name };
        if (correlation is not null)
            description["correlation-filter"] = correlation;
        else
            description["sql-filter"] = new Map { ["expression"] = filter ?? "1=1" };
        if (action is not null)
            description["sql-rule-action"] = new Map { ["expression"] = action };
        return new Map { ["rule-name"] = name, ["rule-description"] = description };
    }

    [Fact]
    public void AddRule_SqlFilterAndAction_StoresWorkingRule()
    {
        var sub = Sub();

        var reply = RuleManagementHandler.AddRule(sub, AddRuleBody("r", "color IN ('red')", "SET user.x = 1"));

        Assert.Equal(200, reply.StatusCode);
        var rule = sub.GetRule("r")!;
        Assert.Equal(FilterType.SqlFilter, rule.FilterType);
        Assert.Equal("color IN ('red')", rule.SqlExpression);
        Assert.Equal("SET user.x = 1", rule.ActionExpression);
        Assert.True(rule.Matches(new BrokeredMessage { ApplicationProperties = { ["color"] = "red" } }));
    }

    [Fact]
    public void AddRule_SymbolKeys_AreAccepted()
    {
        var sub = Sub();
        var body = new Map
        {
            [new Symbol("rule-name")] = "r",
            [new Symbol("rule-description")] = new Map { [new Symbol("sql-filter")] = new Map { [new Symbol("expression")] = "a = 1" } }
        };

        Assert.Equal(200, RuleManagementHandler.AddRule(sub, body).StatusCode);
        Assert.Equal("a = 1", sub.GetRule("r")!.SqlExpression);
    }

    [Fact]
    public void AddRule_CorrelationFilter_MapsEveryField()
    {
        var sub = Sub();
        var correlation = new Map
        {
            ["correlation-id"] = "c", ["message-id"] = "m", ["to"] = "t", ["reply-to"] = "r", ["label"] = "l",
            ["session-id"] = "s", ["reply-to-session-id"] = "rs", ["content-type"] = "ct",
            ["properties"] = new Map { ["k"] = "v" }
        };

        RuleManagementHandler.AddRule(sub, AddRuleBody("corr", correlation: correlation));

        var rule = sub.GetRule("corr")!;
        Assert.Equal(FilterType.CorrelationFilter, rule.FilterType);
        Assert.Equal(("c", "m", "t", "r", "l", "s", "rs", "ct"),
            (rule.CorrelationId, rule.MessageId, rule.To, rule.ReplyTo, rule.Subject, rule.SessionId, rule.ReplyToSessionId, rule.ContentType));
        Assert.Equal("v", rule.CorrelationFilterProperties!["k"]);
    }

    [Fact]
    public void AddRule_InvalidExpression_RepliesAsAzureDoes()
    {
        var sub = Sub();

        var reply = RuleManagementHandler.AddRule(sub, AddRuleBody("bad", "color IN ('a'"));

        // Azure answers the rule manager with a GeneralError ("unable to process the request").
        Assert.Equal(500, reply.StatusCode);
        Assert.Equal("amqp:internal-error", reply.ErrorCondition);
        Assert.StartsWith("The service was unable to process the request; please retry the operation.", reply.Description);
        Assert.Null(sub.GetRule("bad"));
    }

    [Fact]
    public void AddRule_InvalidAction_RepliesAsAzureDoes() =>
        Assert.Equal("amqp:internal-error", RuleManagementHandler.AddRule(Sub(), AddRuleBody("bad", "1=1", "SET")).ErrorCondition);

    [Fact]
    public void AddRule_Duplicate_Replies409EntityAlreadyExists()
    {
        var sub = Sub();
        RuleManagementHandler.AddRule(sub, AddRuleBody("dup"));

        var reply = RuleManagementHandler.AddRule(sub, AddRuleBody("dup"));

        Assert.Equal(409, reply.StatusCode);
        Assert.Equal("com.microsoft:entity-already-exists", reply.ErrorCondition);
    }

    [Fact]
    public void AddRule_MissingSubscription_Replies404() =>
        Assert.Equal(404, RuleManagementHandler.AddRule(null, AddRuleBody("r")).StatusCode);

    [Fact]
    public void RemoveRule_DeletesRule()
    {
        var sub = Sub();
        var reply = RuleManagementHandler.RemoveRule(sub, new Map { ["rule-name"] = "$Default" });

        Assert.Equal(200, reply.StatusCode);
        Assert.Null(sub.GetRule("$Default"));
    }

    [Fact]
    public void RemoveRule_Missing_Replies404NotFound()
    {
        var reply = RuleManagementHandler.RemoveRule(Sub(), new Map { ["rule-name"] = "nope" });

        Assert.Equal(404, reply.StatusCode);
        Assert.Equal("amqp:not-found", reply.ErrorCondition);
    }

    [Fact]
    public void EnumerateRules_EncodesDescribedTypesWithTheSdksCodes()
    {
        var sub = Sub();
        RuleManagementHandler.AddRule(sub, AddRuleBody("sql", "a = 1", "SET user.x = 1"));
        RuleManagementHandler.AddRule(sub, AddRuleBody("true-as-sql", "1=1"));
        RuleManagementHandler.AddRule(sub, AddRuleBody("corr", correlation: new Map { ["label"] = "l", ["properties"] = new Map { ["k"] = "v" } }));

        var reply = RuleManagementHandler.EnumerateRules(sub, new Map { ["top"] = 100, ["skip"] = 0 });

        Assert.Equal(200, reply.StatusCode);
        var rules = ((List)((Map)reply.Body!)["rules"]).Cast<Map>()
            .Select(m => (DescribedValue)m["rule-description"])
            .ToDictionary(d => (string)((List)d.Value)[2]);

        Assert.All(rules.Values, d => Assert.Equal(RuleDescriptionCode, d.Descriptor));

        var defaultRule = (List)rules["$Default"].Value;
        Assert.Equal(TrueFilterCode, ((DescribedValue)defaultRule[0]).Descriptor);
        Assert.Equal(EmptyRuleActionCode, ((DescribedValue)defaultRule[1]).Descriptor);
        Assert.IsType<DateTime>(defaultRule[3]);

        var sql = (List)rules["sql"].Value;
        Assert.Equal(SqlFilterCode, ((DescribedValue)sql[0]).Descriptor);
        Assert.Equal("a = 1", ((List)((DescribedValue)sql[0]).Value)[0]);
        Assert.Equal(SqlRuleActionCode, ((DescribedValue)sql[1]).Descriptor);
        Assert.Equal("SET user.x = 1", ((List)((DescribedValue)sql[1]).Value)[0]);

        // Azure reads a TrueRuleFilter added over AMQP back as SqlRuleFilter("1=1").
        Assert.Equal(SqlFilterCode, ((DescribedValue)((List)rules["true-as-sql"].Value)[0]).Descriptor);

        var corr = (DescribedValue)((List)rules["corr"].Value)[0];
        Assert.Equal(CorrelationFilterCode, corr.Descriptor);
        var fields = (List)corr.Value;
        Assert.Equal(9, fields.Count);
        Assert.Equal("l", fields[4]);
        Assert.Equal("v", ((Map)fields[8])["k"]);
    }

    [Fact]
    public void EnumerateRules_SkipPastEnd_Replies204()
    {
        var reply = RuleManagementHandler.EnumerateRules(Sub(), new Map { ["top"] = 100, ["skip"] = 1 });
        Assert.Equal(204, reply.StatusCode);
    }

    [Fact]
    public void EnumerateRules_TopAndSkip_Page()
    {
        var sub = Sub();
        RuleManagementHandler.AddRule(sub, AddRuleBody("a"));
        RuleManagementHandler.AddRule(sub, AddRuleBody("b"));

        var page = RuleManagementHandler.EnumerateRules(sub, new Map { ["top"] = 1, ["skip"] = 1 });

        Assert.Single((List)((Map)page.Body!)["rules"]);
    }
}
