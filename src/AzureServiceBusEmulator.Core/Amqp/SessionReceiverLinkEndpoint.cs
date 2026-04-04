using global::Amqp;
using global::Amqp.Framing;
using global::Amqp.Listener;
using global::Amqp.Types;
using AzureServiceBusEmulator.Core.Broker;
using Microsoft.Extensions.Logging;
using BrokerSessionState = AzureServiceBusEmulator.Core.Broker.SessionState;

namespace AzureServiceBusEmulator.Core.Amqp;

/// <summary>
/// Session-aware receiver link endpoint. Locks a session on creation and
/// delivers messages only from that session's channel in FIFO order.
/// </summary>
public class SessionReceiverLinkEndpoint : LinkEndpoint
{
    private static readonly ILogger Log = AmqpLog.CreateLogger<SessionReceiverLinkEndpoint>();
    private readonly QueueEntity _queue;
    private readonly BrokerSessionState _session;
    private CancellationTokenSource? _pumpCts;
    private Task? _pumpTask;

    public SessionReceiverLinkEndpoint(QueueEntity queue, BrokerSessionState session)
    {
        _queue = queue;
        _session = session;
    }

    public override void OnFlow(FlowContext flowContext)
    {
        if (flowContext.Link.IsDraining)
        {
            _pumpCts?.Cancel();
            flowContext.Link.CompleteDrain();
            return;
        }

        if (_pumpTask is null || _pumpTask.IsCompleted)
        {
            _pumpCts = new CancellationTokenSource();
            var link = flowContext.Link;
            link.Closed += (_, __) => _pumpCts?.Cancel();
            link.Session.Connection.Closed += (_, __) => _pumpCts?.Cancel();
            _pumpTask = Task.Run(() => SessionPumpAsync(link, _pumpCts.Token));
        }
    }

    private async Task SessionPumpAsync(ListenerLink link, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                // Check AMQPNetLite's internal credit before dequeuing
                if (ReceiverLinkEndpoint.GetLinkCreditStatic(link) <= 0)
                {
                    await Task.Delay(1, ct);
                    continue;
                }

                if (!_session.Messages.Reader.TryRead(out var brokered))
                {
                    // Block until a session message is available rather than busy-polling.
                    await _session.Messages.Reader.WaitToReadAsync(ct);
                    continue;
                }

                _session.DecrementCount();
                brokered.DeliveryCount++;
                brokered.LockedUntil = DateTimeOffset.UtcNow.Add(_queue.LockDuration);
                _queue.TrackPending(brokered);

                var amqpMessage = ReceiverLinkEndpoint.ConvertToAmqpMessage(brokered);

                // Add session ID to message annotations (SDK reads this)
                amqpMessage.MessageAnnotations[new Symbol("x-opt-session-id")] = _session.SessionId;

                try
                {
                    link.SendMessage(amqpMessage);
                }
                catch
                {
                    break;
                }
            }
        }
        catch (OperationCanceledException) { }
        catch { }
    }

    public override void OnDisposition(DispositionContext dispositionContext)
    {
        var lockToken = ReceiverLinkEndpoint.GetLockTokenStatic(dispositionContext.Message);

        if (lockToken is not null && dispositionContext.DeliveryState is not null)
        {
            switch (dispositionContext.DeliveryState)
            {
                case Accepted:
                    _queue.Complete(lockToken);
                    break;
                case Released:
                    _queue.Abandon(lockToken);
                    break;
                case Rejected rejected:
                    _queue.DeadLetter(lockToken,
                        rejected.Error?.Condition?.ToString(),
                        rejected.Error?.Description);
                    break;
                case Modified modified:
                    if (modified.UndeliverableHere == true)
                        _queue.DeadLetter(lockToken, "Undeliverable", "Message marked as undeliverable.");
                    else
                        _queue.Abandon(lockToken);
                    break;
                default:
                    _queue.Complete(lockToken);
                    break;
            }
        }

        dispositionContext.Complete();
    }

    public override void OnLinkClosed(ListenerLink link, Error error)
    {
        _pumpCts?.Cancel();
        _pumpCts?.Dispose();
        _pumpCts = null;

        // Release the session lock when the link closes
        _queue.Sessions?.ReleaseSession(_session.SessionId);

        base.OnLinkClosed(link, error);
    }
}
