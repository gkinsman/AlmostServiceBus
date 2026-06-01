using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Broker.Transactions;

/// <summary>
/// Server-side coordinator for AMQP transactions. Buffers the work of a
/// transaction (sends and settlements, captured as delegates) and applies it
/// atomically on commit or discards it on rollback.
///
/// The manager is deliberately broker-agnostic: callers enlist plain
/// <see cref="Action"/> delegates, so the manager never references AMQP frames
/// or broker entities and can be unit-tested in isolation.
///
/// Transaction ids are globally-unique 16-byte GUIDs, so a single flat table is
/// safe across every connection and entity — which is exactly what cross-entity
/// transactions need (operations against several queues buffered under one id).
/// </summary>
public sealed class TransactionManager
{
    private static readonly ILogger Log = AlmostServiceBus.Core.Amqp.AmqpLog.CreateLogger<TransactionManager>();

    // Keyed by the hex form of the txn-id because byte[] has reference equality.
    private readonly ConcurrentDictionary<string, Transaction> _transactions = new();

    /// <summary>
    /// Allocates a new transaction and returns its id. The id is what the client
    /// receives in the <c>Declared</c> outcome and echoes back on every
    /// transactional transfer, disposition, and the final discharge.
    /// </summary>
    public byte[] Declare()
    {
        var txnId = Guid.NewGuid().ToByteArray();
        _transactions[Key(txnId)] = new Transaction();
        return txnId;
    }

    /// <summary>
    /// Buffers an operation against an open transaction. <paramref name="commit"/>
    /// runs (in enlist order) if the transaction commits; <paramref name="rollback"/>
    /// runs if it rolls back.
    /// </summary>
    /// <exception cref="TransactionNotFoundException">
    /// The id is unknown — either never declared or already discharged.
    /// </exception>
    public void Enlist(byte[] txnId, Action commit, Action? rollback = null)
    {
        if (!_transactions.TryGetValue(Key(txnId), out var txn))
            throw new TransactionNotFoundException(txnId);

        txn.Enlist(commit, rollback);
    }

    /// <summary>
    /// Commits the transaction: runs all buffered commit actions in order, then
    /// removes it. Best-effort — a throwing action is logged and the remaining
    /// actions still run (an in-memory broker cannot truly two-phase-commit).
    /// Returns <c>false</c> for an unknown id.
    /// </summary>
    public bool Commit(byte[] txnId)
    {
        if (!_transactions.TryRemove(Key(txnId), out var txn))
            return false;

        txn.RunCommit(Log);
        return true;
    }

    /// <summary>
    /// Rolls the transaction back: discards buffered commit actions, runs any
    /// rollback actions, then removes it. Never throws. Returns <c>false</c> for
    /// an unknown id.
    /// </summary>
    public bool Rollback(byte[] txnId)
    {
        if (!_transactions.TryRemove(Key(txnId), out var txn))
            return false;

        txn.RunRollback(Log);
        return true;
    }

    private static string Key(byte[] txnId) => Convert.ToHexString(txnId);
}
