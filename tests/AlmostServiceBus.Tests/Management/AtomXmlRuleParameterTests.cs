using AlmostServiceBus.Core.Broker;
using AlmostServiceBus.Core.Management;

namespace AlmostServiceBus.Tests.Management;

/// <summary>SQL filter/action parameters in the Atom XML shape the Azure SDK's admin client sends.</summary>
public class AtomXmlRuleParameterTests
{
    // As serialised by Azure.Messaging.ServiceBus (SqlRuleFilterExtensions / XmlObjectConvertor).
    private const string SdkRuleXml = """
        <entry xmlns="http://www.w3.org/2005/Atom">
          <content type="application/xml">
            <RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"
                             xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
              <Filter i:type="SqlFilter">
                <SqlExpression>a = @a AND b = @b</SqlExpression>
                <Parameters>
                  <KeyValueOfstringanyType>
                    <Key>@a</Key>
                    <Value i:type="l28:string" xmlns:l28="http://www.w3.org/2001/XMLSchema">x</Value>
                  </KeyValueOfstringanyType>
                  <KeyValueOfstringanyType>
                    <Key>@b</Key>
                    <Value i:type="l28:long" xmlns:l28="http://www.w3.org/2001/XMLSchema">5</Value>
                  </KeyValueOfstringanyType>
                </Parameters>
              </Filter>
              <Action i:type="SqlRuleAction">
                <SqlExpression>SET user.ttl = @t</SqlExpression>
                <Parameters>
                  <KeyValueOfstringanyType>
                    <Key>@t</Key>
                    <Value i:type="d4p1:duration" xmlns:d4p1="http://www.w3.org/2001/XMLSchema">PT1H</Value>
                  </KeyValueOfstringanyType>
                </Parameters>
              </Action>
              <Name>params</Name>
            </RuleDescription>
          </content>
        </entry>
        """;

    [Fact]
    public void ReadRuleProperties_ParsesFilterAndActionParameters()
    {
        var props = AtomXmlReader.ReadRuleProperties(SdkRuleXml);

        Assert.Equal(new Dictionary<string, object?> { ["@a"] = "x", ["@b"] = 5L }, props.FilterParameters);
        Assert.Equal(new Dictionary<string, object?> { ["@t"] = TimeSpan.FromHours(1) }, props.ActionParameters);
    }

    [Fact]
    public void ReadRuleProperties_EmptyParametersElement_IsNull()
    {
        const string xml = """
            <entry xmlns="http://www.w3.org/2005/Atom">
              <content type="application/xml">
                <RuleDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"
                                 xmlns:i="http://www.w3.org/2001/XMLSchema-instance">
                  <Filter i:type="SqlFilter"><SqlExpression>a = 1</SqlExpression><Parameters /></Filter>
                  <Name>p</Name>
                </RuleDescription>
              </content>
            </entry>
            """;

        Assert.Null(AtomXmlReader.ReadRuleProperties(xml).FilterParameters);
    }

    public static TheoryData<object> ParameterValues => new()
    {
        "text", 5, 6L, 1.5d, true, new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc), TimeSpan.FromMinutes(90),
    };

    [Theory]
    [MemberData(nameof(ParameterValues))]
    public void WriteThenRead_RoundTripsParameterValueAndType(object value)
    {
        var rule = new RuleEntity
        {
            Name = "r",
            FilterType = FilterType.SqlFilter,
            SqlExpression = "a = @p",
            FilterParameters = new() { ["@p"] = value },
            ActionExpression = "SET user.x = @q",
            ActionParameters = new() { ["@q"] = value },
        };

        var props = AtomXmlReader.ReadRuleProperties(AtomXmlWriter.WriteRuleEntry(rule, "t", "s"));

        Assert.Equal(value, props.FilterParameters!["@p"]);
        Assert.Equal(value.GetType(), props.FilterParameters["@p"]!.GetType());
        Assert.Equal(value, props.ActionParameters!["@q"]);
    }
}
