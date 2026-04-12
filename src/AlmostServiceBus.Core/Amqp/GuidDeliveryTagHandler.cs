using System.Reflection;
using Amqp;
using Amqp.Framing;
using Amqp.Handler;
using Amqp.Listener;
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
    /// Handles Detach frames (remote or local) arriving for links that are still in
    /// Start or AttachSent state. This happens when:
    ///   - A client times out while waiting for a session receiver link's Attach to
    ///     complete (the server is polling for an available session).
    ///   - A connection reset tears down links that haven't completed the AMQP handshake.
    ///
    /// AMQPNetLite's state machine rejects Detach in Start/AttachSent state with
    /// AmqpException, killing the entire connection. For Start state, we use
    /// CompleteAttach(error) to properly register the link in the session's channel
    /// map and close it gracefully. For AttachSent, the link is already registered
    /// (SendAttach called Session.AddLink), so we just transition to Attached.
    ///
    /// Simply setting state=Attached via reflection (without CompleteAttach) skips
    /// Session.AddLink registration → the session's channel map is corrupted → under
    /// load, "session channel N cannot be found" kills the AMQP connection.
    /// </summary>
    private static void HandleLinkClose(Event protocolEvent)
    {
        if (LinkStateField is null || protocolEvent.Link is not Link link)
            return;

        try
        {
            var currentState = (int)(LinkStateField.GetValue(link) ?? -1);

            if (currentState == LinkStateStart)
            {
                // Link is in Start state — Attach handshake never completed.
                // Use CompleteAttach(error) to properly register the link in the
                // session channel map and send Attach+Detach for clean shutdown.
                if (link is ListenerLink listenerLink)
                {
                    try
                    {
                        var responseAttach = new Attach
                        {
                            LinkName = link.Name,
                            Role = link.Role,
                        };
                        listenerLink.CompleteAttach(responseAttach, new Error(new Symbol("amqp:detach-forced"))
                        {
                            Description = "Link detached before attach completed."
                        });
                        Log.LogDebug(
                            "Handled premature close for link '{LinkName}' via CompleteAttach (was Start state, event={EventId})",
                            link.Name, protocolEvent.Id);
                        return;
                    }
                    catch (Exception ex)
                    {
                        Log.LogDebug(ex,
                            "CompleteAttach failed for link '{LinkName}' (Start state), falling back to state transition",
                            link.Name);
                    }
                }

                // Fallback: if CompleteAttach failed (session/connection closing) or
                // link is not a ListenerLink, force state to Attached so AMQPNetLite's
                // Detach processing doesn't throw AmqpException.
                LinkStateField.SetValue(link, (LinkState)LinkStateAttached);
                Log.LogWarning(
                    "Prevented detach crash: transitioned link '{LinkName}' from Start→Attached (event={EventId})",
                    link.Name, protocolEvent.Id);
            }
            else if (currentState == LinkStateAttachSent)
            {
                // AttachSent: our Attach response was sent (Session.AddLink already called),
                // but we haven't received the client's Attach response yet. The session
                // channel map is already consistent. Just transition to Attached so the
                // Detach state machine check passes.
                LinkStateField.SetValue(link, (LinkState)LinkStateAttached);
                Log.LogDebug(
                    "Handled premature close for link '{LinkName}': AttachSent→Attached (event={EventId})",
                    link.Name, protocolEvent.Id);
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "HandleLinkClose failed for link '{LinkName}' (event={EventId})",
                link.Name, protocolEvent.Id);
        }
    }
}
