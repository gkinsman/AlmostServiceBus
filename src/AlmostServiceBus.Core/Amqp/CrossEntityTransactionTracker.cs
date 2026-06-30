using System.Runtime.CompilerServices;
using global::Amqp;

namespace AlmostServiceBus.Core.Amqp;

/// <summary>
/// Tracks per-connection cross-entity-transaction state so the emulator can reproduce real
/// Azure Service Bus's "Local transactions cannot span multiple top-level entities" rejection.
///
/// When <c>EnableCrossEntityTransactions=true</c>, the Azure SDK opens a transaction coordinator
/// link eagerly — before any entity link — and routes every sender and receiver through a single
/// AMQP session (see <c>AmqpConnectionScope</c> in azure-sdk-for-net: "the controller needs to be
/// opened before the link is established"). The first receiver's entity becomes the pinned
/// "send-via" entity; any later receiver on a different top-level entity is rejected by the broker,
/// even outside an active transaction. Senders to other entities are allowed — they are transferred
/// "via" the pinned entity.
///
/// A connection is flagged cross-entity the moment its coordinator link attaches, which is the
/// reliable signal: connections that never open a coordinator carry no constraint, so non-
/// transactional clients are never affected.
/// </summary>
internal static class CrossEntityTransactionTracker
{
    private sealed class State
    {
        public readonly object Gate = new();
        public string? PinnedReceiverEntity;
    }

    private static readonly ConditionalWeakTable<Connection, State> States = new();

    /// <summary>Flags <paramref name="connection"/> as a cross-entity-transaction connection.</summary>
    public static void MarkCrossEntity(Connection connection) =>
        States.GetValue(connection, _ => new State());

    /// <summary>
    /// Admits a receiver attaching to <paramref name="entity"/>. Returns true (and admits) unless the
    /// connection is cross-entity and already pinned to a different top-level entity, in which case it
    /// returns false and reports the pinned entity. The first receiver on a cross-entity connection
    /// pins the entity. Non-cross-entity connections are always admitted.
    /// </summary>
    public static bool TryAdmitReceiver(Connection connection, string entity, out string? pinnedEntity)
    {
        pinnedEntity = null;

        if (!States.TryGetValue(connection, out var state))
        {
            // Not a cross-entity connection — no single-entity receiver constraint applies.
            return true;
        }

        var key = NormalizeTopLevel(entity);
        lock (state.Gate)
        {
            if (state.PinnedReceiverEntity is null)
            {
                state.PinnedReceiverEntity = key;
                return true;
            }

            if (string.Equals(state.PinnedReceiverEntity, key, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }

            pinnedEntity = state.PinnedReceiverEntity;
            return false;
        }
    }

    // The dead-letter sub-queue lives under its parent entity, so a main-queue receiver and a
    // DLQ receiver target the SAME top-level entity and must not be treated as a span violation.
    private static string NormalizeTopLevel(string entity)
    {
        const string dlq = "/$deadletterqueue";
        var trimmed = entity.TrimStart('/');
        return trimmed.EndsWith(dlq, StringComparison.OrdinalIgnoreCase)
            ? trimmed[..^dlq.Length]
            : trimmed;
    }
}
