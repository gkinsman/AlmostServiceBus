using Microsoft.Extensions.Logging;

namespace AlmostServiceBus.Core.Broker.Transactions;

/// <summary>
/// An open transaction: an ordered list of buffered operations. Each operation
/// pairs a commit delegate with an optional rollback delegate. The owning
/// <see cref="TransactionManager"/> runs one set or the other when the client
/// discharges the transaction.
/// </summary>
internal sealed class Transaction
{
    private readonly object _gate = new();
    private readonly List<(Action Commit, Action? Rollback)> _operations = new();

    public void Enlist(Action commit, Action? rollback)
    {
        ArgumentNullException.ThrowIfNull(commit);
        lock (_gate)
        {
            _operations.Add((commit, rollback));
        }
    }

    /// <summary>
    /// Runs every commit action in enlist order. Best-effort: a throwing action
    /// is logged and the remaining actions still run.
    /// </summary>
    public void RunCommit(ILogger log)
    {
        List<(Action Commit, Action? Rollback)> ops;
        lock (_gate)
        {
            ops = new List<(Action, Action?)>(_operations);
        }

        foreach (var op in ops)
        {
            try
            {
                op.Commit();
            }
            catch (Exception ex)
            {
                log.LogWarning(ex, "A buffered transactional operation failed during commit; continuing with the rest.");
            }
        }
    }

    /// <summary>
    /// Runs every rollback action (in enlist order). Commit actions are discarded.
    /// Never throws.
    /// </summary>
    public void RunRollback(ILogger log)
    {
        List<(Action Commit, Action? Rollback)> ops;
        lock (_gate)
        {
            ops = new List<(Action, Action?)>(_operations);
        }

        foreach (var op in ops)
        {
            if (op.Rollback is null) continue;
            try
            {
                op.Rollback();
            }
            catch (Exception ex)
            {
                log.LogDebug(ex, "A buffered transactional rollback action failed; continuing.");
            }
        }
    }
}
