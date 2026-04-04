using AzureServiceBusEmulator.Core.Broker;
using AzureServiceBusEmulator.Core.Management;

namespace AzureServiceBusEmulator.Tests.Management;

public class AtomXmlReaderTests
{
    [Fact]
    public void ReadQueueProperties_ParsesProperties()
    {
        var queue = new QueueEntity("round-trip-queue")
        {
            LockDuration = TimeSpan.FromSeconds(45),
            MaxDeliveryCount = 7,
            MaxSizeInMegabytes = 2048,
            RequiresSession = true,
            DeadLetteringOnMessageExpiration = true,
            DefaultMessageTimeToLive = TimeSpan.FromDays(1),
            EnableBatchedOperations = false,
        };

        var xml = AtomXmlWriter.WriteQueueEntry(queue);
        var props = AtomXmlReader.ReadQueueProperties(xml);

        Assert.Equal(TimeSpan.FromSeconds(45), props.LockDuration);
        Assert.Equal(7, props.MaxDeliveryCount);
        Assert.Equal(2048L, props.MaxSizeInMegabytes);
        Assert.True(props.RequiresSession);
        Assert.True(props.DeadLetteringOnMessageExpiration);
        Assert.Equal(TimeSpan.FromDays(1), props.DefaultMessageTimeToLive);
        Assert.False(props.EnableBatchedOperations);
        Assert.Null(props.ForwardTo);
        Assert.Null(props.UserMetadata);
    }

    [Fact]
    public void ReadQueueProperties_ParsesOptionalFields()
    {
        var queue = new QueueEntity("queue-with-opts")
        {
            ForwardTo = "other-queue",
            UserMetadata = "my-metadata",
        };

        var xml = AtomXmlWriter.WriteQueueEntry(queue);
        var props = AtomXmlReader.ReadQueueProperties(xml);

        Assert.Equal("other-queue", props.ForwardTo);
        Assert.Equal("my-metadata", props.UserMetadata);
    }

    [Fact]
    public void ReadTopicProperties_ParsesProperties()
    {
        var topic = new TopicEntity("round-trip-topic")
        {
            MaxSizeInMegabytes = 4096,
            DefaultMessageTimeToLive = TimeSpan.FromHours(12),
            EnableBatchedOperations = false,
            UserMetadata = "topic-meta",
        };

        var xml = AtomXmlWriter.WriteTopicEntry(topic);
        var props = AtomXmlReader.ReadTopicProperties(xml);

        Assert.Equal(4096L, props.MaxSizeInMegabytes);
        Assert.Equal(TimeSpan.FromHours(12), props.DefaultMessageTimeToLive);
        Assert.False(props.EnableBatchedOperations);
        Assert.Equal("topic-meta", props.UserMetadata);
    }

    [Fact]
    public void ReadSubscriptionProperties_ParsesForwardTo()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic")
        {
            ForwardTo = "forward-target",
            MaxDeliveryCount = 5,
            LockDuration = TimeSpan.FromSeconds(60),
            RequiresSession = true,
            DeadLetteringOnMessageExpiration = true,
            EnableBatchedOperations = true,
            DefaultMessageTimeToLive = TimeSpan.FromDays(7),
        };

        var xml = AtomXmlWriter.WriteSubscriptionEntry(sub);
        var props = AtomXmlReader.ReadSubscriptionProperties(xml);

        Assert.Equal("forward-target", props.ForwardTo);
        Assert.Equal(5, props.MaxDeliveryCount);
        Assert.Equal(TimeSpan.FromSeconds(60), props.LockDuration);
        Assert.True(props.RequiresSession);
        Assert.True(props.DeadLetteringOnMessageExpiration);
        Assert.True(props.EnableBatchedOperations);
        Assert.Equal(TimeSpan.FromDays(7), props.DefaultMessageTimeToLive);
    }

    [Fact]
    public void ReadRuleProperties_ParsesTrueFilter()
    {
        var rule = new RuleEntity
        {
            Name = "$Default",
            FilterType = FilterType.TrueFilter,
        };

        var xml = AtomXmlWriter.WriteRuleEntry(rule);
        var props = AtomXmlReader.ReadRuleProperties(xml);

        Assert.Equal("$Default", props.Name);
        Assert.Equal(FilterType.TrueFilter, props.FilterType);
        Assert.Null(props.SqlExpression);
        Assert.Null(props.CorrelationId);
    }

    [Fact]
    public void ReadRuleProperties_ParsesSqlFilter()
    {
        var rule = new RuleEntity
        {
            Name = "color-filter",
            FilterType = FilterType.SqlFilter,
            SqlExpression = "color = 'blue'",
            ActionExpression = "SET sys.label = 'handled'",
        };

        var xml = AtomXmlWriter.WriteRuleEntry(rule);
        var props = AtomXmlReader.ReadRuleProperties(xml);

        Assert.Equal("color-filter", props.Name);
        Assert.Equal(FilterType.SqlFilter, props.FilterType);
        Assert.Equal("color = 'blue'", props.SqlExpression);
        Assert.Equal("SET sys.label = 'handled'", props.ActionExpression);
    }

    [Fact]
    public void ReadRuleProperties_ParsesCorrelationFilter()
    {
        var rule = new RuleEntity
        {
            Name = "corr-filter",
            FilterType = FilterType.CorrelationFilter,
            CorrelationId = "my-correlation-id",
        };

        var xml = AtomXmlWriter.WriteRuleEntry(rule);
        var props = AtomXmlReader.ReadRuleProperties(xml);

        Assert.Equal("corr-filter", props.Name);
        Assert.Equal(FilterType.CorrelationFilter, props.FilterType);
        Assert.Equal("my-correlation-id", props.CorrelationId);
    }

    [Fact]
    public void ReadQueueProperties_MaxValueTimeSpan_RoundTrips()
    {
        var queue = new QueueEntity("ttl-queue")
        {
            DefaultMessageTimeToLive = TimeSpan.MaxValue,
        };

        var xml = AtomXmlWriter.WriteQueueEntry(queue);
        var props = AtomXmlReader.ReadQueueProperties(xml);

        Assert.Equal(TimeSpan.MaxValue, props.DefaultMessageTimeToLive);
    }

    [Fact]
    public void ReadSubscriptionProperties_WithDefaultRuleDescription_ParsesRule()
    {
        // Simulate the XML the Azure SDK sends when calling
        // CreateSubscriptionAsync(options, CreateRuleOptions) — it embeds the rule under
        // <DefaultRuleDescription> inside the <SubscriptionDescription>.
        var xml = """
            <entry xmlns="http://www.w3.org/2005/Atom">
              <content type="application/xml">
                <SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"
                                         xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <LockDuration>PT30S</LockDuration>
                  <RequiresSession>false</RequiresSession>
                  <DefaultMessageTimeToLive>P10675199DT2H48M5.4775807S</DefaultMessageTimeToLive>
                  <MaxDeliveryCount>10</MaxDeliveryCount>
                  <DefaultRuleDescription>
                    <Name>my-guid-rule</Name>
                    <Filter i:type="SqlFilter">
                      <SqlExpression>ClientId = 27</SqlExpression>
                    </Filter>
                  </DefaultRuleDescription>
                </SubscriptionDescription>
              </content>
            </entry>
            """;

        var props = AtomXmlReader.ReadSubscriptionProperties(xml);

        Assert.NotNull(props.DefaultRule);
        Assert.Equal("my-guid-rule", props.DefaultRule!.Name);
        Assert.Equal(FilterType.SqlFilter, props.DefaultRule.FilterType);
        Assert.Equal("ClientId = 27", props.DefaultRule.SqlExpression);
    }

    [Fact]
    public void ReadSubscriptionProperties_WithoutDefaultRuleDescription_DefaultRuleIsNull()
    {
        var sub = new AzureServiceBusEmulator.Core.Broker.SubscriptionEntity("sub1", "topic1");
        var xml = AtomXmlWriter.WriteSubscriptionEntry(sub);

        var props = AtomXmlReader.ReadSubscriptionProperties(xml);

        Assert.Null(props.DefaultRule);
    }
}
