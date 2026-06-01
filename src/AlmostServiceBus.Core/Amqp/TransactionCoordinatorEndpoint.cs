using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Transactions;
using global::Amqp.Types;
using AlmostServiceBus.Core.Broker.Transactions;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Server-side endpoint for a transaction coordinator link. The client opens
/// this link with a <see cref="Coordinator"/> target and sends it two kinds of
/// control messages:
///   • <see cref="Declare"/>   — start a transaction. We allocate an id and
///                                settle the delivery with a <see cref="Declared"/>
///                                outcome carrying that id.
///   • <see cref="Discharge"/> — finish a transaction. We commit (apply all
///                                buffered work) or roll back (discard it) and
///                                settle the delivery with <see cref="Accepted"/>.
///
/// The buffered work itself is captured elsewhere — <see cref="SenderLinkEndpoint"/>
/// (transactional sends) and the receiver endpoints (transactional settlements)
/// enlist delegates with the shared <see cref="TransactionManager"/>.
/// </summary>
public sealed class TransactionCoordinatorEndpoint : LinkEndpoint
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<TransactionCoordinatorEndpoint>();

    private readonly TransactionManager _transactions;

    public TransactionCoordinatorEndpoint(TransactionManager transactions)
    {
        _transactions = transactions;
    }

    public override void OnMessage(MessageContext messageContext)
    {
        try
        {
            switch (UnwrapBody(messageContext.Message))
            {
                case Declare:
                    var txnId = _transactions.Declare();
                    Log.LogDebug("TXN declare → {TxnId}", Convert.ToHexString(txnId));
                    messageContext.Link.DisposeMessage(messageContext.Message, new Declared { TxnId = txnId }, true);
                    break;

                case Discharge discharge:
                    var applied = discharge.Fail
                        ? _transactions.Rollback(discharge.TxnId)
                        : _transactions.Commit(discharge.TxnId);
                    Log.LogDebug("TXN discharge {TxnId} fail={Fail} applied={Applied}",
                        Convert.ToHexString(discharge.TxnId), discharge.Fail, applied);

                    if (applied)
                    {
                        messageContext.Link.DisposeMessage(messageContext.Message, new Accepted(), true);
                    }
                    else
                    {
                        messageContext.Link.DisposeMessage(messageContext.Message, new Rejected
                        {
                            Error = new Error(new Symbol("amqp:transaction-unknown-id"))
                            {
                                Description = "Unknown or already-discharged transaction id."
                            }
                        }, true);
                    }
                    break;

                default:
                    Log.LogWarning("TXN coordinator received an unrecognised control message: {Body}",
                        messageContext.Message.Body?.GetType().Name ?? "null");
                    messageContext.Complete(new Error(new Symbol("amqp:not-implemented"))
                    {
                        Description = "Only declare and discharge are supported on a coordinator link."
                    });
                    break;
            }
        }
        catch (Exception ex)
        {
            Log.LogWarning(ex, "TXN coordinator failed to process a control message.");
            messageContext.Complete(new Error(new Symbol("amqp:internal-error"))
            {
                Description = "Failed to process transaction control message."
            });
        }
    }

    // AMQPNetLite usually decodes the body's described type directly, but some
    // encoders wrap it in an AmqpValue / DescribedValue. Peel those so we always
    // compare against the concrete Declare/Discharge type.
    private static object? UnwrapBody(Message message)
    {
        var body = message.Body;
        if (body is AmqpValue value)
            body = value.Value;
        return body;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        // No-op: the coordinator link is driven entirely by incoming control messages.
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        dispositionContext.Complete();
    }
}
