using System.Xml.Linq;
using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Management;

namespace AlmostServiceBus.Tests.Management;

public class AtomXmlWriterTests
{
    private static readonly XNamespace Atom = "http://www.w3.org/2005/Atom";
    private static readonly XNamespace Sb = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";

    [Fact]
    public void WriteQueueEntry_ContainsQueueDescription()
    {
        var queue = new QueueEntity("my-queue")
        {
            LockDuration = TimeSpan.FromSeconds(30),
            MaxDeliveryCount = 5,
            MaxSizeInMegabytes = 2048,
            RequiresSession = false,
            DeadLetteringOnMessageExpiration = true,
            EnablePartitioning = true,
            EnableExpress = true,
            EnableBatchedOperations = true,
        };

        var xml = AtomXmlWriter.WriteQueueEntry(queue);
        var doc = XDocument.Parse(xml);

        var entry = doc.Root!;
        Assert.Equal(Atom + "entry", entry.Name);

        var title = entry.Element(Atom + "title");
        Assert.NotNull(title);
        Assert.Equal("my-queue", title!.Value);

        var content = entry.Element(Atom + "content");
        Assert.NotNull(content);
        Assert.Equal("application/xml", content!.Attribute("type")?.Value);

        var queueDesc = content.Element(Sb + "QueueDescription");
        Assert.NotNull(queueDesc);

        var lockDuration = queueDesc!.Element(Sb + "LockDuration");
        Assert.NotNull(lockDuration);
        Assert.Equal("PT30S", lockDuration!.Value);

        var maxDeliveryCount = queueDesc.Element(Sb + "MaxDeliveryCount");
        Assert.NotNull(maxDeliveryCount);
        Assert.Equal("5", maxDeliveryCount!.Value);

        var deadLettering = queueDesc.Element(Sb + "DeadLetteringOnMessageExpiration");
        Assert.NotNull(deadLettering);
        Assert.Equal("true", deadLettering!.Value);
    }

    [Fact]
    public void WriteQueueEntry_OmitsOptionalElementsWhenNull()
    {
        var queue = new QueueEntity("my-queue")
        {
            ForwardTo = null,
            UserMetadata = null,
        };

        var xml = AtomXmlWriter.WriteQueueEntry(queue);
        var doc = XDocument.Parse(xml);
        var queueDesc = doc.Descendants(Sb + "QueueDescription").Single();

        Assert.Null(queueDesc.Element(Sb + "ForwardTo"));
        Assert.Null(queueDesc.Element(Sb + "UserMetadata"));
    }

    [Fact]
    public void WriteQueueEntry_IncludesOptionalElementsWhenSet()
    {
        var queue = new QueueEntity("my-queue")
        {
            ForwardTo = "other-queue",
            UserMetadata = "some-metadata",
        };

        var xml = AtomXmlWriter.WriteQueueEntry(queue);
        var doc = XDocument.Parse(xml);
        var queueDesc = doc.Descendants(Sb + "QueueDescription").Single();

        Assert.Equal("other-queue", queueDesc.Element(Sb + "ForwardTo")?.Value);
        Assert.Equal("some-metadata", queueDesc.Element(Sb + "UserMetadata")?.Value);
    }

    [Fact]
    public void WriteTopicEntry_ContainsTopicDescription()
    {
        var topic = new TopicEntity("my-topic")
        {
            MaxSizeInMegabytes = 4096,
            EnablePartitioning = true,
            EnableExpress = true,
            EnableBatchedOperations = false,
            SupportOrdering = false
        };

        var xml = AtomXmlWriter.WriteTopicEntry(topic);
        var doc = XDocument.Parse(xml);

        var entry = doc.Root!;
        Assert.Equal(Atom + "entry", entry.Name);

        var title = entry.Element(Atom + "title");
        Assert.Equal("my-topic", title?.Value);

        var topicDesc = doc.Descendants(Sb + "TopicDescription").Single();
        Assert.Equal("4096", topicDesc.Element(Sb + "MaxSizeInMegabytes")?.Value);
        Assert.Equal("true", topicDesc.Element(Sb + "EnablePartitioning")?.Value);
        Assert.Equal("true", topicDesc.Element(Sb + "EnableExpress")?.Value);
        Assert.Equal("false", topicDesc.Element(Sb + "EnableBatchedOperations")?.Value);
        Assert.Equal("true", topicDesc.Element(Sb + "SupportOrdering")?.Value);
    }

    [Fact]
    public void WriteSubscriptionEntry_ContainsSubscriptionDescription()
    {
        var sub = new SubscriptionEntity("my-sub", "my-topic")
        {
            MaxDeliveryCount = 3,
            ForwardTo = "target-queue",
            LockDuration = TimeSpan.FromMinutes(1),
        };

        var xml = AtomXmlWriter.WriteSubscriptionEntry(sub);
        var doc = XDocument.Parse(xml);

        var entry = doc.Root!;
        Assert.Equal(Atom + "entry", entry.Name);

        var subDesc = doc.Descendants(Sb + "SubscriptionDescription").Single();
        Assert.Equal("3", subDesc.Element(Sb + "MaxDeliveryCount")?.Value);
        Assert.Equal("target-queue", subDesc.Element(Sb + "ForwardTo")?.Value);
        Assert.Equal("PT1M", subDesc.Element(Sb + "LockDuration")?.Value);
    }

    [Fact]
    public void WriteRuleEntry_ContainsRuleDescription()
    {
        var rule = new RuleEntity
        {
            Name = "my-rule",
            FilterType = FilterType.TrueFilter,
        };

        var xml = AtomXmlWriter.WriteRuleEntry(rule);
        var doc = XDocument.Parse(xml);

        var entry = doc.Root!;
        Assert.Equal(Atom + "entry", entry.Name);

        var ruleDesc = doc.Descendants(Sb + "RuleDescription").Single();
        var filter = ruleDesc.Element(Sb + "Filter");
        Assert.NotNull(filter);

        var xsiType = filter!.Attribute(XNamespace.Get("http://www.w3.org/2001/XMLSchema-instance") + "type");
        Assert.NotNull(xsiType);
        Assert.Equal("TrueFilter", xsiType!.Value);

        var nameEl = ruleDesc.Element(Sb + "Name");
        Assert.Equal("my-rule", nameEl?.Value);
    }

    [Fact]
    public void WriteRuleEntry_SqlFilter_ContainsSqlExpression()
    {
        var rule = new RuleEntity
        {
            Name = "sql-rule",
            FilterType = FilterType.SqlFilter,
            SqlExpression = "color = 'red'",
        };

        var xml = AtomXmlWriter.WriteRuleEntry(rule);
        var doc = XDocument.Parse(xml);
        var ruleDesc = doc.Descendants(Sb + "RuleDescription").Single();
        var filter = ruleDesc.Element(Sb + "Filter");
        Assert.Equal("SqlFilter", filter?.Attribute(XNamespace.Get("http://www.w3.org/2001/XMLSchema-instance") + "type")?.Value);
        Assert.Equal("color = 'red'", filter?.Element(Sb + "SqlExpression")?.Value);
    }

    [Fact]
    public void WriteQueueFeed_WrapsMultipleEntries()
    {
        var queues = new[]
        {
            new QueueEntity("queue-a"),
            new QueueEntity("queue-b"),
        };

        var xml = AtomXmlWriter.WriteQueueFeed(queues);
        var doc = XDocument.Parse(xml);

        var feed = doc.Root!;
        Assert.Equal(Atom + "feed", feed.Name);

        var entries = feed.Elements(Atom + "entry").ToList();
        Assert.Equal(2, entries.Count);
    }

    [Fact]
    public void WriteTopicFeed_WrapsMultipleEntries()
    {
        var topics = new[]
        {
            new TopicEntity("topic-a"),
            new TopicEntity("topic-b"),
        };

        var xml = AtomXmlWriter.WriteTopicFeed(topics);
        var doc = XDocument.Parse(xml);
        var entries = doc.Root!.Elements(Atom + "entry").ToList();
        Assert.Equal(2, entries.Count);
    }

    [Fact]
    public void WriteSubscriptionFeed_WrapsMultipleEntries()
    {
        var subs = new[]
        {
            new SubscriptionEntity("sub-a", "my-topic"),
            new SubscriptionEntity("sub-b", "my-topic"),
        };

        var xml = AtomXmlWriter.WriteSubscriptionFeed(subs);
        var doc = XDocument.Parse(xml);
        var entries = doc.Root!.Elements(Atom + "entry").ToList();
        Assert.Equal(2, entries.Count);
    }

    [Fact]
    public void WriteRuleFeed_WrapsMultipleEntries()
    {
        var rules = new[]
        {
            new RuleEntity { Name = "rule-a" },
            new RuleEntity { Name = "rule-b" },
        };

        var xml = AtomXmlWriter.WriteRuleFeed(rules);
        var doc = XDocument.Parse(xml);
        var entries = doc.Root!.Elements(Atom + "entry").ToList();
        Assert.Equal(2, entries.Count);
    }

    [Fact]
    public void FormatTimeSpan_MaxValue_ReturnsAzureRepresentation()
    {
        var result = AtomXmlWriter.FormatTimeSpan(TimeSpan.MaxValue);
        Assert.Equal("P10675199DT2H48M5.4775807S", result);
    }

    [Fact]
    public void FormatTimeSpan_NormalSpan_UsesXmlConvert()
    {
        var ts = TimeSpan.FromSeconds(30);
        var result = AtomXmlWriter.FormatTimeSpan(ts);
        Assert.Equal("PT30S", result);
    }
}
