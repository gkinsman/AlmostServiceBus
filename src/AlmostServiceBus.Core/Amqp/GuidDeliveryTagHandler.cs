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
/// 3. Prevents "OnDetach is not valid under state: Start/AttachSent" from killing
///    connections when clients send Detach for links whose Attach hasn't been
///    completed yet (e.g. session receivers waiting for a session to become available),
///    or during connection resets where links are locally closed in incomplete states.
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
    // connection. We intercept here (fires BEFORE the state machine check) to prevent it.
    private static readonly FieldInfo? LinkStateField =
        typeof(Link).GetField("state", BindingFlags.NonPublic | BindingFlags.Instance);

    // LinkState enum values from AMQP 1.0 spec / AMQPNetLite internals:
    //   Start=0, AttachSent=1, AttachReceived=2, Attached=3,
    //   DetachPipe=4, DetachSent=5, DetachReceived=6, End=7
    private const int LinkStateStart = 0;
    private const int LinkStateAttachSent = 1;
    // DetachSent = 5: semantically "we have sent Detach, waiting for response".
    // When AMQPNetLite's OnDetach runs on a link in this state, it transitions
    // directly to End WITHOUT sending a Detach response frame. This is crucial
    // for links whose Attach handshake never completed: the client may have
    // already End'd the whole AMQP session before our Detach response could
    // arrive, and any frame on an End'd session causes the client to throw
    // "session channel N cannot be found" and close the connection.
    private const int LinkStateDetachSent = 5;

    static GuidDeliveryTagHandler()
    {
        var deliveryType = typeof(Link).Assembly.GetType("Amqp.Delivery");
        TagProperty = deliveryType?.GetProperty("Tag");
        MessageProperty = deliveryType?.GetProperty("Message");
    }

    public bool CanHandle(EventId id) =>
        id == EventId.SendDelivery ||
        id == EventId.ConnectionRemoteClose ||
        id == EventId.LinkRemoteClose ||
        id == EventId.LinkLocalClose;

    public void Handle(Event protocolEvent)
    {
        if (protocolEvent.Id == EventId.ConnectionRemoteClose)
        {
            HandleConnectionRemoteClose(protocolEvent);
            return;
        }

        if (protocolEvent.Id == EventId.LinkRemoteClose || protocolEvent.Id == EventId.LinkLocalClose)
        {
            HandleLinkClose(protocolEvent);
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
        catch (Exception ex)
        {
            Log.LogDebug(ex, "HandleConnectionRemoteClose: error during CBS cleanup");
        }
    }

    /// <summary>
    /// Handles Detach frames (remote or local) for links in pre-Attached states
    /// (Start, AttachSent). This happens when:
    ///   - A client times out while waiting for a session receiver link's Attach to
    ///     complete (the server is polling for an available session).
    ///   - A connection reset tears down links that haven't completed the AMQP handshake.
    ///
    /// AMQPNetLite's state machine rejects Detach in these states with AmqpException,
    /// killing the entire connection.
    ///
    /// We transition to DetachSent (not Attached). In AMQPNetLite's OnDetach flow:
    ///   - Attached + receive Detach → DetachReceived → sends Detach response → End
    ///   - DetachSent + receive Detach → End (no response frame sent)
    ///
    /// Sending a response Detach is risky when the client has concurrently End'd the
    /// whole AMQP session: the response frame arrives at a channel the client has
    /// already removed from its map, causing "session channel N cannot be found" at
    /// the client, which tears down the connection. Using DetachSent avoids sending
    /// any frame and still allows AMQPNetLite to cleanly remove the link from the
    /// session's local channel map (Session.RemoveLink is called unconditionally).
    /// </summary>
    private static void HandleLinkClose(Event protocolEvent)
    {
        if (LinkStateField is null || protocolEvent.Link is not Link link)
            return;

        try
        {
            var currentState = (int)(LinkStateField.GetValue(link) ?? -1);

            if (currentState == LinkStateStart || currentState == LinkStateAttachSent)
            {
                LinkStateField.SetValue(link, (LinkState)LinkStateDetachSent);
                Log.LogDebug(
                    "Handled premature close for link '{LinkName}': state {OldState}→DetachSent (event={EventId})",
                    link.Name, currentState, protocolEvent.Id);
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "HandleLinkClose failed for link '{LinkName}' (event={EventId})",
                link.Name, protocolEvent.Id);
        }
    }
}
