using AlmostServiceBus.Core.Broker.Transactions;

namespace AlmostServiceBus.Tests.Broker.Transactions;

public class TransactionManagerTests
{
    [Fact]
    public void Declare_issues_a_non_empty_id()
    {
        var mgr = new TransactionManager();

        var txnId = mgr.Declare();

        Assert.NotNull(txnId);
        Assert.NotEmpty(txnId);
    }

    [Fact]
    public void Declare_issues_a_unique_id_each_time()
    {
        var mgr = new TransactionManager();

        var first = mgr.Declare();
        var second = mgr.Declare();

        Assert.False(first.AsSpan().SequenceEqual(second), "Declare must return distinct transaction ids");
    }

    [Fact]
    public void Commit_runs_buffered_actions_in_enlist_order()
    {
        var mgr = new TransactionManager();
        var txnId = mgr.Declare();
        var order = new List<int>();

        mgr.Enlist(txnId, commit: () => order.Add(1));
        mgr.Enlist(txnId, commit: () => order.Add(2));
        mgr.Enlist(txnId, commit: () => order.Add(3));

        var committed = mgr.Commit(txnId);

        Assert.True(committed);
        Assert.Equal(new[] { 1, 2, 3 }, order);
    }

    [Fact]
    public void Commit_runs_each_action_exactly_once()
    {
        var mgr = new TransactionManager();
        var txnId = mgr.Declare();
        var runs = 0;

        mgr.Enlist(txnId, commit: () => runs++);
        mgr.Commit(txnId);

        Assert.Equal(1, runs);
    }

    [Fact]
    public void Rollback_does_not_run_commit_actions_but_runs_rollback_actions()
    {
        var mgr = new TransactionManager();
        var txnId = mgr.Declare();
        var committed = false;
        var rolledBack = false;

        mgr.Enlist(txnId, commit: () => committed = true, rollback: () => rolledBack = true);

        var ok = mgr.Rollback(txnId);

        Assert.True(ok);
        Assert.False(committed);
        Assert.True(rolledBack);
    }

    [Fact]
    public void Commit_of_unknown_id_returns_false()
    {
        var mgr = new TransactionManager();

        Assert.False(mgr.Commit(new byte[] { 1, 2, 3, 4 }));
    }

    [Fact]
    public void Rollback_of_unknown_id_returns_false()
    {
        var mgr = new TransactionManager();

        Assert.False(mgr.Rollback(new byte[] { 1, 2, 3, 4 }));
    }

    [Fact]
    public void Enlist_on_unknown_id_throws()
    {
        var mgr = new TransactionManager();

        Assert.Throws<TransactionNotFoundException>(() => mgr.Enlist(new byte[] { 9 }, commit: () => { }));
    }

    [Fact]
    public void Transaction_is_removed_after_commit_so_second_commit_returns_false()
    {
        var mgr = new TransactionManager();
        var txnId = mgr.Declare();
        mgr.Enlist(txnId, commit: () => { });

        Assert.True(mgr.Commit(txnId));
        Assert.False(mgr.Commit(txnId));
    }

    [Fact]
    public void Transaction_is_removed_after_rollback_so_enlist_then_throws()
    {
        var mgr = new TransactionManager();
        var txnId = mgr.Declare();

        Assert.True(mgr.Rollback(txnId));
        Assert.Throws<TransactionNotFoundException>(() => mgr.Enlist(txnId, commit: () => { }));
    }

    [Fact]
    public void Commit_runs_remaining_actions_even_when_one_throws()
    {
        var mgr = new TransactionManager();
        var txnId = mgr.Declare();
        var lastRan = false;

        mgr.Enlist(txnId, commit: () => throw new InvalidOperationException("boom"));
        mgr.Enlist(txnId, commit: () => lastRan = true);

        var committed = mgr.Commit(txnId);

        Assert.True(committed);
        Assert.True(lastRan);
    }
}
