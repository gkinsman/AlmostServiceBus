using System.Reflection;
using Amqp;
using Amqp.Framing;
using Amqp.Handler;
using Amqp.Types;
using Microsoft.Extensions.Logging;
using EventId = Amqp.Handler.EventId;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// AMQPNetLite IHandler that:
/// 1. Rewrites outgoing delivery tags from 4-byte integers to 16-byte GUIDs
///    (the Azure SDK reads the delivery tag as LockTokenGuid — if it's not
///    16 bytes, the SDK treats the message as "peeked" and rejects settlement).
/// 2. Handles connection close events to clean up CBS connection tracking.
/// 3. Prevents "OnDetach is not valid under state: Start" from killing connections
///    when clients send Detach for links whose Attach hasn't been completed yet
///    (e.g. session receivers waiting for a session to become available).
///
/// Uses reflection because AMQPNetLite's Delivery class and Link.state are internal.
/// </summary>
public class GuidDeliveryTagHandler : IHandler
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<GuidDeliveryTagHandler>();

    private static readonly PropertyInfo? TagProperty;
    private static readonly PropertyInfo? MessageProperty;

    // Link.state is a private field of type LinkState. When a Detach frame arrives for
    // a link in Start state, AMQPNetLite throws AmqpException which kills the entire
    // connection. By transitioning the link to Attached state in this handler (which
    // fires BEFORE the state machine check), we prevent the exception.
    private static readonly FieldInfo? LinkStateField =
        typeof(Link).GetField("state", BindingFlags.NonPublic | BindingFlags.Instance);

    // LinkState.Start = 0, LinkState.Attached = 3
    private const int LinkStateStart = 0;
    private const int LinkStateAttached = 3;

    static GuidDeliveryTagHandler()
    {
        var deliveryType = typeof(Link).Assembly.GetType("Amqp.Delivery");
        TagProperty = deliveryType?.GetProperty("Tag");
        MessageProperty = deliveryType?.GetProperty("Message");
    }

    public bool CanHandle(EventId id) =>
        id == EventId.SendDelivery ||
        id == EventId.ConnectionRemoteClose ||
        id == EventId.LinkRemoteClose;

    public void Handle(Event protocolEvent)
    {
        if (protocolEvent.Id == EventId.ConnectionRemoteClose)
        {
            HandleConnectionRemoteClose(protocolEvent);
            return;
        }

        if (protocolEvent.Id == EventId.LinkRemoteClose)
        {
            HandleLinkRemoteClose(protocolEvent);
            return;
        }

        if (protocolEvent.Context is null || TagProperty is null) return;

        try
        {
            // Read the lock token from the message's x-opt-lock-token annotation
            var message = MessageProperty?.GetValue(protocolEvent.Context) as Message;
            if (message?.MessageAnnotations?[new Symbol("x-opt-lock-token")] is Guid lockGuid)
            {
                TagProperty.SetValue(protocolEvent.Context, lockGuid.ToByteArray());
            }
            else
            {
                TagProperty.SetValue(protocolEvent.Context, Guid.NewGuid().ToByteArray());
            }
        }
        catch { /* best effort — if this fails, delivery tag stays as 4-byte int */ }
    }

    private static void HandleConnectionRemoteClose(Event protocolEvent)
    {
        try
        {
            // Clean up CBS connection tracking when a connection is closed remotely.
            if (protocolEvent.Context is Connection connection)
            {
                CbsRequestProcessor.RemoveConnection(connection);
            }
        }
        catch { /* best effort cleanup */ }
    }

    /// <summary>
    /// Handles Detach frames arriving for links that are still in Start state.
    /// This happens when a client times out or disconnects while waiting for a
    /// session receiver link's Attach to complete (the server is polling for an
    /// available session). AMQPNetLite's state machine rejects Detach in Start
    /// state with AmqpException, killing the entire connection. By transitioning
    /// the link to Attached state here, the Detach is handled gracefully.
    /// </summary>
    private static void HandleLinkRemoteClose(Event protocolEvent)
    {
        if (LinkStateField is null || protocolEvent.Link is not Link link)
            return;

        try
        {
            var currentState = (int)(LinkStateField.GetValue(link) ?? -1);
            if (currentState == LinkStateStart)
            {
                LinkStateField.SetValue(link, (LinkState)LinkStateAttached);
                Log.LogWarning(
                    "Prevented OnDetach crash: transitioned link '{LinkName}' from Start→Attached to allow graceful Detach",
                    link.Name);
            }
        }
        catch { /* best effort — if reflection fails, the original error behavior persists */ }
    }
}
