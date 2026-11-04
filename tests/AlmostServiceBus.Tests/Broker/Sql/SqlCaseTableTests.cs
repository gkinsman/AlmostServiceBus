using System.Text;
using System.Text.Json;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Broker.Sql;
using AlmostServiceBus.Tests.Shared;

namespace AlmostServiceBus.Tests.Broker.Sql;

/// <summary>
/// Runs every case of the Azure-recorded table (tests/shared/sql-compat-cases.json) through the broker: rules are
/// added to a subscription, the case's message is published to the topic, and the delivered and dead-lettered
/// copies are compared with what Azure produced. A failure here is an emulator bug, never a table edit.
/// </summary>
public class SqlCaseTableTests
{
    public static TheoryData<string> CaseIds()
    {
        var data = new TheoryData<string>();
        foreach (var c in SqlCompatCases.Load().Where(c => c.SkipEmulator != true)) data.Add(c.Id);
        return data;
    }

    [Theory]
    [MemberData(nameof(CaseIds))]
    public void EmulatorMatchesAzure(string id)
    {
        var c = SqlCompatCases.Get(id);
        var actual = Run(c);
        var problems = SqlCompatComparer.Compare(c.Expected, actual);
        Assert.True(problems.Count == 0,
            $"{c.Id}{(c.Note is null ? "" : " — " + c.Note)}\n  {string.Join("\n  ", problems)}\n" +
            $"expected: {JsonSerializer.Serialize(c.Expected, SqlCompatCases.JsonOptions)}\n" +
            $"actual:   {JsonSerializer.Serialize(actual, SqlCompatCases.JsonOptions)}");
    }

    private static SqlCompatExpected Run(SqlCompatCase c)
    {
        var topic = new TopicEntity("sqlcompat");
        var sub = topic.AddSubscription("case");
        sub.DeadLetteringOnFilterEvaluationExceptions = c.Subscription.DeadLetterOnFilterExceptions;
        sub.RemoveRule("$Default");

        for (var k = 0; k < c.Rules.Count; k++)
        {
            var r = c.Rules[k];
            var rule = new RuleEntity
            {
                Name = $"r{k}",
                ActionExpression = r.Action,
                ActionParameters = Clr(r.ActionParameters),
            };
            if (r.Correlation == true)
            {
                rule.FilterType = FilterType.CorrelationFilter;
                rule.CorrelationFilterProperties = new() { ["caseId"] = c.Id };
            }
            else
            {
                rule.FilterType = FilterType.SqlFilter;
                rule.SqlExpression = c.GuardedFilter(r);
                rule.FilterParameters = Clr(r.FilterParameters);
            }

            try
            {
                sub.AddOrUpdateRule(rule);
            }
            catch (ArgumentException ex)
            {
                return new SqlCompatExpected
                {
                    Outcome = "Rejected",
                    Error = new SqlCompatError { Type = nameof(ArgumentException), Message = ex.Message }
                };
            }
        }

        topic.Publish(BuildMessage(c));

        var result = new SqlCompatExpected();
        while (sub.Queue.TryDequeueImmediate() is { } copy)
            result.Copies.Add(new SqlCompatCopy { User = User(copy), Sys = Sys(copy) });
        while (sub.Queue.DeadLetterQueue.TryDequeueImmediate() is { } dead)
            result.DeadLettered.Add(new SqlCompatDeadLetter
            {
                Reason = dead.DeadLetterReason, Description = dead.DeadLetterErrorDescription, User = User(dead)
            });
        result.Outcome = result.DeadLettered.Count > 0 ? "DeadLettered" : result.Copies.Count > 0 ? "Match" : "NoMatch";
        return result;
    }

    private static Dictionary<string, object?>? Clr(Dictionary<string, TypedValue>? values) =>
        values?.ToDictionary(kv => kv.Key, kv => kv.Value.ToClr());

    // Builds the message the probe sent through the .NET SDK, including the SDK's PartitionKey = SessionId.
    private static BrokeredMessage BuildMessage(SqlCompatCase c)
    {
        string? Sys(string key) => c.Message.Sys.GetValueOrDefault(key);
        var message = new BrokeredMessage
        {
            Body = Encoding.UTF8.GetBytes("body"),
            MessageId = Sys("MessageId") ?? "m-" + c.Id,
            Subject = Sys("Label"),
            To = Sys("To"),
            ReplyTo = Sys("ReplyTo"),
            CorrelationId = Sys("CorrelationId"),
            ContentType = Sys("ContentType"),
            ReplyToSessionId = Sys("ReplyToSessionId"),
            SessionId = Sys("SessionId"),
            PartitionKey = Sys("PartitionKey") ?? Sys("SessionId"),
        };
        if (Sys("TimeToLive") is { } ttl)
            message.TimeToLive = TimeSpan.Parse(ttl, System.Globalization.CultureInfo.InvariantCulture);

        message.ApplicationProperties["caseId"] = c.Id;
        foreach (var (key, value) in c.Message.User)
            message.ApplicationProperties[key] = value.ToClr()!;
        return message;
    }

    private static readonly HashSet<string> NotUser = ["caseId", "DeadLetterReason", "DeadLetterErrorDescription", "DeadLetterSource"];

    private static Dictionary<string, TypedValue> User(BrokeredMessage m) =>
        m.ApplicationProperties.Where(kv => !NotUser.Contains(kv.Key))
            .ToDictionary(kv => kv.Key, kv => TypedValue.From(SqlValues.FromMessage(kv.Value)));

    private static Dictionary<string, string?> Sys(BrokeredMessage m)
    {
        var all = new Dictionary<string, string?>
        {
            ["Subject"] = m.Subject, ["To"] = m.To, ["CorrelationId"] = m.CorrelationId, ["MessageId"] = m.MessageId,
            ["ReplyTo"] = m.ReplyTo, ["ReplyToSessionId"] = m.ReplyToSessionId, ["SessionId"] = m.SessionId,
            ["ContentType"] = m.ContentType, ["PartitionKey"] = m.PartitionKey,
            ["TimeToLive"] = m.TimeToLive == TimeSpan.MaxValue ? null : m.TimeToLive.ToString(),
            ["ScheduledEnqueueTime"] = m.ScheduledEnqueueTimeUtc?.ToString("o"),
        };
        return all.Where(kv => kv.Value is not null).ToDictionary();
    }
}
