using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Management;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Net;
using System.Text;

namespace AlmostServiceBus.Tests.Management;

public class ManagementApiRuleTests : IAsyncLifetime
{
    private const string TopicName = "rules-topic";
    private const string SubName = "rules-sub";

    private IHost _host = null!;
    private HttpClient _client = null!;
    private readonly NamespaceRegistry _registry = new();

    public async Task InitializeAsync()
    {
        _host = await new HostBuilder()
            .ConfigureWebHost(webBuilder =>
            {
                webBuilder.UseTestServer();
                webBuilder.ConfigureServices(services => services.AddRouting());
                webBuilder.Configure(app =>
                {
                    app.UseRouting();
                    app.UseEndpoints(endpoints => endpoints.MapServiceBusManagementApi(_registry));
                });
            })
            .StartAsync();
        _client = _host.GetTestClient();
        _client.DefaultRequestHeaders.Host = "test-ns.servicebus.windows.net";

        // Pre-create topic and subscription
        var topic = new TopicEntity(TopicName);
        var topicXml = AtomXmlWriter.WriteTopicEntry(topic);
        await _client.PutAsync($"/{TopicName}", new StringContent(topicXml, Encoding.UTF8, "application/atom+xml"));

        var sub = new SubscriptionEntity(SubName, TopicName);
        var subXml = AtomXmlWriter.WriteSubscriptionEntry(sub);
        await _client.PutAsync($"/{TopicName}/Subscriptions/{SubName}", new StringContent(subXml, Encoding.UTF8, "application/atom+xml"));
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    private StringContent RuleXmlBody(string ruleName)
    {
        var rule = new RuleEntity { Name = ruleName, FilterType = FilterType.TrueFilter };
        var xml = AtomXmlWriter.WriteRuleEntry(rule, TopicName, SubName);
        return new StringContent(xml, Encoding.UTF8, "application/atom+xml");
    }

    [Fact]
    public async Task GetDefaultRule_Returns200()
    {
        var response = await _client.GetAsync($"/{TopicName}/Subscriptions/{SubName}/Rules/$Default");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("RuleDescription", body);
        Assert.Contains("$Default", body);
    }

    [Fact]
    public async Task ListRules_ReturnsFeed()
    {
        var response = await _client.GetAsync($"/{TopicName}/Subscriptions/{SubName}/Rules");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("feed", body);
        Assert.Contains("$Default", body);
    }

    [Fact]
    public async Task CreateRule_Returns201()
    {
        var response = await _client.PutAsync(
            $"/{TopicName}/Subscriptions/{SubName}/Rules/my-rule",
            RuleXmlBody("my-rule"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("RuleDescription", body);
        Assert.Contains("my-rule", body);
    }

    [Fact]
    public async Task DeleteRule_Returns200()
    {
        // Create a rule to delete
        await _client.PutAsync(
            $"/{TopicName}/Subscriptions/{SubName}/Rules/rule-to-delete",
            RuleXmlBody("rule-to-delete"));

        var response = await _client.DeleteAsync($"/{TopicName}/Subscriptions/{SubName}/Rules/rule-to-delete");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
