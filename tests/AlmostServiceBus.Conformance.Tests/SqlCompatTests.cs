using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Azure.Messaging.ServiceBus;
using Azure.Messaging.ServiceBus.Administration;
using AlmostServiceBus.TestHost;
using AlmostServiceBus.Tests.Shared;
using Xunit.Abstractions;

namespace AlmostServiceBus.Conformance.Tests;

/// <summary>
/// Runs the shared SQL filter/action table (tests/shared/sql-compat-cases.json) through the SDK against the
/// emulator, and against real Azure when a connection string is configured (<see cref="RealAsbConnection"/>),
/// then diffs both with the recorded answers. With <c>ASB_COMPAT_RECORD=1</c> Azure's results are written back
/// into the table. Every case shares one topic, guarded by <c>user.caseId</c>, which is deleted afterwards.
/// </summary>
public class SqlCompatTests(ITestOutputHelper output)
{
    private static readonly HashSet<string> NotUser = ["caseId", "DeadLetterReason", "DeadLetterErrorDescription", "DeadLetterSource"];

    [Fact]
    public async Task SqlCompatTable_EmulatorAndAzureAgreeWithTable()
    {
        var cases = SqlCompatCases.Load();

        await using var fixture = new ServiceBusEmulatorFixture();
        await fixture.StartAsync();
        var emulator = await RunAsync(fixture.ConnectionString, cases.Where(c => c.SkipEmulator != true).ToList(),
            new ServiceBusRetryOptions { MaxRetries = 0, TryTimeout = TimeSpan.FromSeconds(10) });

        Dictionary<string, SqlCompatExpected>? azure = null;
        if (RealAsbConnection.ConnectionString is { } azureConnection)
        {
            azure = await RunAsync(azureConnection, cases, new ServiceBusRetryOptions { MaxRetries = 2 });
            output.WriteLine($"Ran {cases.Count} cases against Azure.");
        }
        else
        {
            output.WriteLine(RealAsbConnection.NotConfigured + " — comparing the emulator with the recorded table only.");
        }

        var record = azure is not null && Environment.GetEnvironmentVariable("ASB_COMPAT_RECORD") == "1";
        if (record)
        {
            foreach (var c in cases)
            {
                c.Expected = azure![c.Id];
                c.Verified = "azure";
            }

            SqlCompatCases.Save(cases);
            output.WriteLine($"Recorded Azure's results into {SqlCompatCases.SourcePath()}.");
        }

        var report = new StringBuilder("| id | table | azure | emulator |\n|---|---|---|---|\n");
        var failures = 0;
        foreach (var c in cases)
        {
            var azureProblems = azure is null ? [] : SqlCompatComparer.Compare(c.Expected, azure[c.Id]);
            var emulatorProblems = emulator.TryGetValue(c.Id, out var e) ? SqlCompatComparer.Compare(c.Expected, e) : [];
            if (azureProblems.Count == 0 && emulatorProblems.Count == 0)
                continue;

            failures++;
            report.Append($"| {c.Id} | {c.Expected.Outcome} | {Summary(azure?.GetValueOrDefault(c.Id), azureProblems)} " +
                          $"| {Summary(e, emulatorProblems)} |\n");
        }

        output.WriteLine(failures == 0 ? $"All {cases.Count} cases agree." : report.ToString());
        Assert.True(failures == 0, $"{failures} case(s) diverge from the recorded table:\n{report}");
    }

    private static string Summary(SqlCompatExpected? result, List<string> problems) =>
        result is null ? "—" : problems.Count == 0 ? "ok" : $"{result.Outcome}: {string.Join("; ", problems)}".Replace("|", "\\|").Replace("\n", " ");

    // ── One run of the table against a namespace ─────────────────────────────

    private static async Task<Dictionary<string, SqlCompatExpected>> RunAsync(
        string connectionString, IReadOnlyList<SqlCompatCase> cases, ServiceBusRetryOptions retry)
    {
        var admin = new ServiceBusAdministrationClient(connectionString);
        await using var client = new ServiceBusClient(connectionString, new ServiceBusClientOptions { RetryOptions = retry });
        var topic = $"sqlcompat-{Guid.NewGuid():N}"[..18];
        var results = new ConcurrentDictionary<string, SqlCompatExpected>();
        var subscription = cases.Select((c, i) => (c.Id, Name: $"c{i:D3}")).ToDictionary(x => x.Id, x => x.Name);

        await admin.CreateTopicAsync(topic);
        try
        {
            await Parallel.ForEachAsync(cases, new ParallelOptions { MaxDegreeOfParallelism = 8 }, async (c, ct) =>
            {
                try
                {
                    var options = new CreateSubscriptionOptions(topic, subscription[c.Id])
                    {
                        EnableDeadLetteringOnFilterEvaluationExceptions = c.Subscription.DeadLetterOnFilterExceptions
                    };
                    await admin.CreateSubscriptionAsync(options, BuildRule(c, 0), ct);
                    for (var k = 1; k < c.Rules.Count; k++)
                        await admin.CreateRuleAsync(topic, subscription[c.Id], BuildRule(c, k), ct);
                }
                catch (Exception ex) when (ex is ArgumentException or ServiceBusException)
                {
                    results[c.Id] = new SqlCompatExpected
                    {
                        Outcome = "Rejected",
                        Error = new SqlCompatError { Type = ex.GetType().Name, Message = StripServiceTail(ex.Message) }
                    };
                }
            });

            await using (var sender = client.CreateSender(topic))
            {
                foreach (var c in cases.Where(c => !results.ContainsKey(c.Id)))
                    await sender.SendMessageAsync(BuildMessage(c));
            }

            await Task.Delay(TimeSpan.FromSeconds(2));

            await Parallel.ForEachAsync(cases.Where(c => !results.ContainsKey(c.Id)),
                new ParallelOptions { MaxDegreeOfParallelism = 16 }, async (c, ct) =>
                {
                    var result = new SqlCompatExpected();
                    foreach (var m in await DrainAsync(client, topic, subscription[c.Id], SubQueue.None, ct))
                        result.Copies.Add(new SqlCompatCopy { User = User(m), Sys = Sys(m) });
                    foreach (var m in await DrainAsync(client, topic, subscription[c.Id], SubQueue.DeadLetter, ct))
                        result.DeadLettered.Add(new SqlCompatDeadLetter
                        {
                            Reason = m.DeadLetterReason, Description = m.DeadLetterErrorDescription, User = User(m)
                        });
                    result.Outcome = result.DeadLettered.Count > 0 ? "DeadLettered" : result.Copies.Count > 0 ? "Match" : "NoMatch";
                    results[c.Id] = result;
                });
        }
        finally
        {
            await admin.DeleteTopicAsync(topic);
        }

        return new Dictionary<string, SqlCompatExpected>(results);
    }

    private static CreateRuleOptions BuildRule(SqlCompatCase c, int index)
    {
        var rule = c.Rules[index];
        RuleFilter filter;
        if (rule.Correlation == true)
        {
            filter = new CorrelationRuleFilter { ApplicationProperties = { ["caseId"] = c.Id } };
        }
        else
        {
            var sql = new SqlRuleFilter(c.GuardedFilter(rule));
            foreach (var (key, value) in rule.FilterParameters ?? []) sql.Parameters[key] = value.ToClr();
            filter = sql;
        }

        var options = new CreateRuleOptions($"r{index}", filter);
        if (rule.Action is not null)
        {
            var action = new SqlRuleAction(rule.Action);
            foreach (var (key, value) in rule.ActionParameters ?? []) action.Parameters[key] = value.ToClr();
            options.Action = action;
        }

        return options;
    }

    private static ServiceBusMessage BuildMessage(SqlCompatCase c)
    {
        string? Sys(string key) => c.Message.Sys.GetValueOrDefault(key);
        var message = new ServiceBusMessage("body")
        {
            MessageId = Sys("MessageId") ?? "m-" + c.Id,
            Subject = Sys("Label"),
            To = Sys("To"),
            ReplyTo = Sys("ReplyTo"),
            CorrelationId = Sys("CorrelationId"),
            ContentType = Sys("ContentType"),
            ReplyToSessionId = Sys("ReplyToSessionId"),
        };
        if (Sys("SessionId") is { } sessionId) message.SessionId = sessionId;
        if (Sys("PartitionKey") is { } partitionKey) message.PartitionKey = partitionKey;
        if (Sys("TimeToLive") is { } ttl) message.TimeToLive = TimeSpan.Parse(ttl, CultureInfo.InvariantCulture);

        message.ApplicationProperties["caseId"] = c.Id;
        foreach (var (key, value) in c.Message.User)
            message.ApplicationProperties[key] = value.ToClr();
        return message;
    }

    private static async Task<List<ServiceBusReceivedMessage>> DrainAsync(
        ServiceBusClient client, string topic, string subscription, SubQueue subQueue, CancellationToken ct)
    {
        await using var receiver = client.CreateReceiver(topic, subscription, new ServiceBusReceiverOptions
        {
            ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete, SubQueue = subQueue
        });
        var all = new List<ServiceBusReceivedMessage>();
        while (true)
        {
            var wait = subQueue == SubQueue.None && all.Count == 0 ? TimeSpan.FromSeconds(3) : TimeSpan.FromSeconds(1);
            var batch = await receiver.ReceiveMessagesAsync(10, wait, ct);
            if (batch.Count == 0) return all;
            all.AddRange(batch);
        }
    }

    private static Dictionary<string, TypedValue> User(ServiceBusReceivedMessage m) =>
        m.ApplicationProperties.Where(kv => !NotUser.Contains(kv.Key)).ToDictionary(kv => kv.Key, kv => TypedValue.From(kv.Value));

    private static Dictionary<string, string?> Sys(ServiceBusReceivedMessage m)
    {
        var all = new Dictionary<string, string?>
        {
            ["Subject"] = m.Subject, ["To"] = m.To, ["CorrelationId"] = m.CorrelationId, ["MessageId"] = m.MessageId,
            ["ReplyTo"] = m.ReplyTo, ["ReplyToSessionId"] = m.ReplyToSessionId, ["SessionId"] = m.SessionId,
            ["ContentType"] = m.ContentType, ["PartitionKey"] = m.PartitionKey,
            ["TimeToLive"] = m.TimeToLive == TimeSpan.MaxValue ? null : m.TimeToLive.ToString(),
            ["ScheduledEnqueueTime"] = m.ScheduledEnqueueTime == default ? null : m.ScheduledEnqueueTime.ToString("o"),
        };
        return all.Where(kv => kv.Value is not null).ToDictionary();
    }

    // Azure appends tracking details (and the SDK the HTTP status and body) after the service's message.
    private static string StripServiceTail(string message) =>
        Regex.Replace(message, @"\s*(TrackingId:|Status: \d{3}).*$", "", RegexOptions.Singleline);
}
