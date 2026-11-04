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

    [Fact]
    public async Task CreateRule_InvalidSqlFilter_Returns400AndAddsNoRule()
    {
        var rule = new RuleEntity { Name = "bad-rule", FilterType = FilterType.SqlFilter, SqlExpression = "color IN ('red'" };
        var xml = AtomXmlWriter.WriteRuleEntry(rule, TopicName, SubName);

        var response = await _client.PutAsync(
            $"/{TopicName}/Subscriptions/{SubName}/Rules/bad-rule",
            new StringContent(xml, Encoding.UTF8, "application/atom+xml"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(_registry.GetOrCreate("test-ns").GetSubscription(TopicName, SubName)!.GetRule("bad-rule"));
        Assert.Contains("There was an error parsing the SQL expression. [Token line=2, column=0, Token in error= , Additional details= syntax error, unexpected EOF",
            await response.Content.ReadAsStringAsync());
    }

    private Task<HttpResponseMessage> PutRule(RuleEntity rule, bool update = false)
    {
        var request = new HttpRequestMessage(HttpMethod.Put, $"/{TopicName}/Subscriptions/{SubName}/Rules/{rule.Name}")
        {
            Content = new StringContent(AtomXmlWriter.WriteRuleEntry(rule, TopicName, SubName), Encoding.UTF8, "application/atom+xml")
        };
        if (update) request.Headers.Add("If-Match", "*");
        return _client.SendAsync(request);
    }

    [Fact]
    public async Task CreateRule_WithParameters_GetRuleReturnsSameTypes()
    {
        await PutRule(new RuleEntity
        {
            Name = "params", FilterType = FilterType.SqlFilter, SqlExpression = "a = @a AND b = @b",
            FilterParameters = new() { ["@a"] = "x", ["@b"] = 5L },
            ActionExpression = "SET user.t = @t", ActionParameters = new() { ["@t"] = TimeSpan.FromHours(1) },
        });

        var response = await _client.GetAsync($"/{TopicName}/Subscriptions/{SubName}/Rules/params");
        var props = AtomXmlReader.ReadRuleProperties(await response.Content.ReadAsStringAsync());

        Assert.Equal(new Dictionary<string, object?> { ["@a"] = "x", ["@b"] = 5L }, props.FilterParameters);
        Assert.Equal(new Dictionary<string, object?> { ["@t"] = TimeSpan.FromHours(1) }, props.ActionParameters);
    }

    [Fact]
    public async Task CreateRule_UnusedParameter_Returns400WithAzureText()
    {
        var response = await PutRule(new RuleEntity
        {
            Name = "unused", FilterType = FilterType.SqlFilter, SqlExpression = "a = 1",
            FilterParameters = new() { ["@x"] = 1 },
        });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("The specified parameter (@x) in Parameters property was not used in the expression specified in SqlExpression.",
            await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task CreateRule_MultiLineAzureMessage_KeepsItsCarriageReturn()
    {
        var response = await PutRule(new RuleEntity { Name = "sys", FilterType = FilterType.SqlFilter, SqlExpression = "sys.Subject IS NULL" });

        var detail = System.Xml.Linq.XDocument.Parse(await response.Content.ReadAsStringAsync()).Root!.Element("Detail")!.Value;
        Assert.EndsWith("BrokeredMessage class.\r\nParameter name: qualifiedPropertyName", detail);
    }

    [Fact]
    public async Task CreateRule_InvalidAction_Returns400()
    {
        var response = await PutRule(new RuleEntity { Name = "bad-action", FilterType = FilterType.TrueFilter, ActionExpression = "SET" });

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Contains("expecting IDENTIFIER, or DELIMITED_IDENTIFIER", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task CreateRule_Duplicate_Returns409_UpdateWithIfMatchSucceeds()
    {
        var rule = new RuleEntity { Name = "dup", FilterType = FilterType.SqlFilter, SqlExpression = "a = 1" };
        Assert.Equal(HttpStatusCode.Created, (await PutRule(rule)).StatusCode);

        var duplicate = await PutRule(rule);
        Assert.Equal(HttpStatusCode.Conflict, duplicate.StatusCode);
        var detail = System.Xml.Linq.XDocument.Parse(await duplicate.Content.ReadAsStringAsync()).Root!.Element("Detail")!.Value;
        Assert.Equal($"The messaging entity 'test-ns:Topic:{TopicName}|{SubName}|dup' already exists.", detail);

        Assert.Equal(HttpStatusCode.OK, (await PutRule(rule, update: true)).StatusCode);
    }

    [Fact]
    public async Task CreateSubscription_WithInvalidDefaultRuleSqlFilter_Returns400AndCreatesNothing()
    {
        var xml = """
            <entry xmlns="http://www.w3.org/2005/Atom">
              <content type="application/xml">
                <SubscriptionDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"
                                         xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <DefaultRuleDescription>
                    <Name>bad-default</Name>
                    <Filter i:type="SqlFilter">
                      <SqlExpression>"unterminated = 'x'</SqlExpression>
                    </Filter>
                  </DefaultRuleDescription>
                </SubscriptionDescription>
              </content>
            </entry>
            """;

        var response = await _client.PutAsync(
            $"/{TopicName}/Subscriptions/bad-sub",
            new StringContent(xml, Encoding.UTF8, "application/atom+xml"));

        Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        Assert.Null(_registry.GetOrCreate("test-ns").GetSubscription(TopicName, "bad-sub"));
    }
}
