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

public class ManagementApiQueueTests : IAsyncLifetime
{
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
    }

    public async Task DisposeAsync()
    {
        await _host.StopAsync();
        _host.Dispose();
    }

    private StringContent QueueXmlBody(string queueName)
    {
        var queue = new QueueEntity(queueName);
        var xml = AtomXmlWriter.WriteQueueEntry(queue);
        return new StringContent(xml, Encoding.UTF8, "application/atom+xml");
    }

    [Fact]
    public async Task CreateQueue_Returns201()
    {
        var response = await _client.PutAsync("/my-queue", QueueXmlBody("my-queue"));

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("QueueDescription", body);
        Assert.Contains("my-queue", body);
    }

    [Fact]
    public async Task GetQueue_Returns200()
    {
        await _client.PutAsync("/get-queue", QueueXmlBody("get-queue"));

        var response = await _client.GetAsync("/get-queue");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("QueueDescription", body);
        Assert.Contains("get-queue", body);
    }

    [Fact]
    public async Task GetQueue_Returns404_WhenNotFound()
    {
        var response = await _client.GetAsync("/nonexistent-queue");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("could not be found", body);
    }

    [Fact]
    public async Task DeleteQueue_Returns200()
    {
        await _client.PutAsync("/delete-queue", QueueXmlBody("delete-queue"));

        var response = await _client.DeleteAsync("/delete-queue");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task DeleteQueue_Returns404_WhenNotFound()
    {
        var response = await _client.DeleteAsync("/nonexistent-delete-queue");

        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        var body = await response.Content.ReadAsStringAsync();
        Assert.Contains("could not be found", body);
    }
}
