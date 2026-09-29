namespace AlmostServiceBus.Tests.TestHelpers;

/// <summary>
/// Runs blocking test work (barrier waits, spin loops) on its own thread rather than the thread
/// pool. Parking dozens of pool threads at a <see cref="Barrier"/> starves the pool for seconds
/// while it slowly injects more, which delays timers and <c>Task.Delay</c> continuations in every
/// other test running in parallel and makes the timing-sensitive ones fail on small CI runners.
/// </summary>
internal static class DedicatedThread
{
    public static Task Run(Action action) =>
        Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
}
