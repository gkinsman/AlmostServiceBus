using Amqp.Framing;
using Amqp.Types;
using AlmostServiceBus.Core.Amqp;

namespace AlmostServiceBus.Tests.Amqp;

/// <summary>
/// Parsing of settlement outcomes sent by non-.NET SDKs. Found by the client SDK smoke tests
/// (tests/client-sdk-smoke): rhea (Node.js) sends the dead-letter map with <em>string</em> keys,
/// which AMQPNetLite's <see cref="Fields"/> indexer refuses; indexing threw inside OnDisposition,
/// the settlement was never sent and the client's dead-letter call timed out.
/// </summary>
public class ReceiverLinkEndpointOutcomeTests
{
    private static Fields WithStringKeys(params (string Key, object Value)[] entries)
    {
        var fields = new Fields();
        // Bypass the Symbol-only indexer the way a decoded frame from rhea does.
        var dict = (IDictionary<object, object>)fields;
        foreach (var (key, value) in entries)
            dict.Add(key, value);
        return fields;
    }

    [Fact]
    public void ExtractDeadLetterInfo_AcceptsStringKeys()
    {
        var rejected = new Rejected
        {
            Error = new Error(new Symbol("com.microsoft:dead-letter"))
            {
                Description = "schema mismatch",
                Info = WithStringKeys(("DeadLetterReason", "ValidationFailed"), ("DeadLetterErrorDescription", "schema mismatch")),
            },
        };

        var (reason, description) = ReceiverLinkEndpoint.ExtractDeadLetterInfoStatic(rejected);

        Assert.Equal("ValidationFailed", reason);
        Assert.Equal("schema mismatch", description);
    }

    [Fact]
    public void ExtractDeadLetterInfo_AcceptsSymbolKeys()
    {
        var info = new Fields
        {
            [new Symbol("DeadLetterReason")] = "Poison",
            [new Symbol("DeadLetterErrorDescription")] = "bad payload",
        };
        var rejected = new Rejected { Error = new Error(new Symbol("com.microsoft:dead-letter")) { Info = info } };

        var (reason, description) = ReceiverLinkEndpoint.ExtractDeadLetterInfoStatic(rejected);

        Assert.Equal("Poison", reason);
        Assert.Equal("bad payload", description);
    }

    [Fact]
    public void ExtractRejectedProperties_AcceptsStringKeys_AndSkipsWellKnownOnes()
    {
        var rejected = new Rejected
        {
            Error = new Error(new Symbol("com.microsoft:dead-letter"))
            {
                Info = WithStringKeys(("DeadLetterReason", "r"), ("attempt", 3), ("region", "uk")),
            },
        };

        var props = ReceiverLinkEndpoint.ExtractRejectedProperties(rejected);

        Assert.NotNull(props);
        Assert.Equal(2, props.Count);
        Assert.Equal(3, props["attempt"]);
        Assert.Equal("uk", props["region"]);
    }

    [Fact]
    public void ExtractMessageAnnotationProperties_AcceptsStringKeys()
    {
        var modified = new Modified { MessageAnnotations = WithStringKeys(("retryCount", 2)) };

        var props = ReceiverLinkEndpoint.ExtractMessageAnnotationProperties(modified);

        Assert.NotNull(props);
        Assert.Equal(2, props["retryCount"]);
    }
}
