using AlmostServiceBus.Core.Broker;

namespace AlmostServiceBus.Tests.Broker.Sql;

/// <summary>The reproduction from GitHub issue #192, verbatim apart from collecting mismatches for the assertion.</summary>
public class Issue192ReproTests
{
    [Fact]
    public void SqlInAndDelimitedIdentifiers_MatchOnlyTheMatchingValue()
    {
        var mismatches = new List<string>();
        var failures = 0;
        foreach (var expression in new[]
        {
            "messageType = 'Created'", // Supported syntax: positive/negative control.
            "messageType IN ('Created', 'Updated')",
            "\"message-type\" = 'Created'",
            "[message-type] = 'Created'",
            "\"message-type\" IN ('Created', 'Updated')",
        })
        {
            var rule = new RuleEntity
            {
                Name = "filter", FilterType = FilterType.SqlFilter, SqlExpression = expression
            };
            foreach (var value in new[] { "Created", "Unrelated" })
            {
                var message = new BrokeredMessage
                {
                    ApplicationProperties = new()
                    {
                        ["messageType"] = value, ["message-type"] = value
                    }
                };
                var actual = rule.Matches(message);
                var expected = value == "Created";
                if (actual != expected) failures++;
                if (actual != expected)
                    mismatches.Add($"SQL {expression}; value={value}: actual={actual}, expected={expected}");
            }
        }

        Assert.Equal(0, failures);
        Assert.Empty(mismatches);
    }
}
