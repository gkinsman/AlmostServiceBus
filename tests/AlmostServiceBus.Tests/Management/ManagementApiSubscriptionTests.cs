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

public class ManagementApiSubscriptionTests : IAsyncLifetime
{
    private const string TopicName = "my-topic";

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

        // Pre-create the topic so subscription tests can proceed
        var topic = new TopicEntity(TopicName);
        var topicXml = AtomXmlWriter.WriteTopicEntry(topic);
        await _client.PutAsync($"/{TopicName}", new StringContent(topicXml, Encoding.UTF8, "application/atom+xml"));
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    private StringContent SubscriptionXmlBody(string subName)
    {
        // Build a minimal SubscriptionDescription atom entry
        var sub = new SubscriptionEntity(subName, TopicName);
        var xml = AtomXmlWriter.WriteSubscriptionEntry(sub);
        return new StringContent(xml, Encoding.UTF8, "application/atom+xml");
    }

    [Fact]
    public async Task CreateSubscription_Returns201()
    {
        var response = await _client.PutAsync($"/{TopicName}/Subscriptions/sub-create", SubscriptionXmlBody("sub-create"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("SubscriptionDescription", body);
        Assert.Contains("sub-create", body);
    }

    [Fact]
    public async Task GetSubscription_Returns200()
    {
        await _client.PutAsync($"/{TopicName}/Subscriptions/sub-get", SubscriptionXmlBody("sub-get"));

        var response = await _client.GetAsync($"/{TopicName}/Subscriptions/sub-get");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("SubscriptionDescription", body);
        Assert.Contains("sub-get", body);
    }

    [Fact]
    public async Task GetSubscription_Returns404_WhenNotFound()
    {
        var response = await _client.GetAsync($"/{TopicName}/Subscriptions/nonexistent-sub");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("could not be found", body);
    }

    [Fact]
    public async Task UpdateSubscription_WithIfMatch_Returns200()
    {
        await _client.PutAsync($"/{TopicName}/Subscriptions/sub-update", SubscriptionXmlBody("sub-update"));

        var request = new HttpRequestMessage(HttpMethod.Put, $"/{TopicName}/Subscriptions/sub-update")
        {
            Content = SubscriptionXmlBody("sub-update"),
            Headers = { { "If-Match", "*" } }
        };
        var response = await _client.SendAsync(request);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("SubscriptionDescription", body);
    }

    [Fact]
    public async Task DeleteSubscription_Returns200()
    {
        await _client.PutAsync($"/{TopicName}/Subscriptions/sub-delete", SubscriptionXmlBody("sub-delete"));

        var response = await _client.DeleteAsync($"/{TopicName}/Subscriptions/sub-delete");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }
}
