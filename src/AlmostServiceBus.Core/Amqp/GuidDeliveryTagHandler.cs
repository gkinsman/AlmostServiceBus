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

    // LinkState enum values (internal to AMQPNetLite)
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
        id == EventId.SessionLocalClose ||
        id == EventId.SessionRemoteClose ||
        id == EventId.LinkRemoteClose;

    public void Handle(Event protocolEvent)
    {
        if (protocolEvent.Id == EventId.ConnectionRemoteClose)
        {
            HandleConnectionRemoteClose(protocolEvent);
            return;
        }

        if (protocolEvent.Id == EventId.SessionLocalClose || protocolEvent.Id == EventId.SessionRemoteClose)
        {
            // Track AMQP session closures to diagnose "session channel not found" errors.
            try
            {
                var direction = protocolEvent.Id == EventId.SessionLocalClose ? "LOCAL" : "REMOTE";
                Log.LogWarning("AMQP session {Direction} close", direction);
            }
            catch { }
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
    /// Handles Detach frames arriving for links in states that don't accept Detach.
    /// AMQPNetLite's state machine throws AmqpException for Detach in Start or
    /// AttachSent state, killing the entire AMQP connection.
    ///
    /// Two cases:
    /// - Start: link attach was never completed (server polling for a session).
    ///   Fix: call CompleteAttach with an error to properly allocate a session handle.
    /// - AttachSent: server sent Attach but client's Detach arrived before the Attach
    ///   response was processed. The session handle was already allocated by SendAttach,
    ///   so directly transitioning to Attached is safe (no channel corruption).
    /// </summary>
    private static void HandleLinkRemoteClose(Event protocolEvent)
    {
        if (LinkStateField is null || protocolEvent.Link is not Link link)
            return;

        try
        {
            var currentState = (int)(LinkStateField.GetValue(link) ?? -1);

            // Handle ANY state that isn't fully Attached (3) or beyond (DetachSent=4+).
            // States 0 (Start), 1 (AttachSent), 2 (AttachReceived) all reject Detach.
            if (currentState >= 0 && currentState < LinkStateAttached)
            {
                if (currentState == LinkStateStart && link is ListenerLink listenerLink)
                {
                    // Start: no session handle allocated yet. Use CompleteAttach to
                    // properly allocate via Session.AddLink before the Detach runs.
                    try
                    {
                        listenerLink.CompleteAttach(new Attach() { LinkName = link.Name }, new Error(new Symbol("amqp:detach-forced"))
                        {
                            Description = "Link detached by client while attach was pending."
                        });
                        return;
                    }
                    catch { /* fall through to state hack */ }
                }

                // AttachSent/AttachReceived (or Start fallback): session handle was
                // already allocated by SendAttach, so directly setting to Attached
                // keeps channel accounting correct and lets Detach proceed.
                LinkStateField.SetValue(link, (LinkState)LinkStateAttached);
            }
        }
        catch { /* best effort — if reflection fails, original error behavior persists */ }
    }
}
