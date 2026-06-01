namespace AlmostServiceBus.Core.Broker.Transactions;

/// <summary>
/// Thrown when transactional work references a transaction id that the
/// <see cref="TransactionManager"/> does not know about — either it was never
/// declared, or it has already been committed or rolled back.
/// </summary>
public sealed class TransactionNotFoundException : Exception
{
    public TransactionNotFoundException(byte[] txnId)
        : base($"No open transaction with id '{Convert.ToHexString(txnId)}'.")
    {
    }
}
