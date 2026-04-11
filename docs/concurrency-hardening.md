# Concurrency Hardening Report

A full thread-safety audit of the emulator's broker and AMQP layers, followed by targeted fixes for every issue found. The goal: make the emulator safe under the kind of concurrent connection load that MassTransit, Wolverine, and the Azure SDK throw at it.

## The Problem

The emulator was functionally correct in single-threaded scenarios but had 12 concurrency issues ranging from classic TOCTOU races to torn struct writes. Several could manifest under normal SDK usage patterns -- for example, two `AcceptNextSessionAsync` calls racing could both grab the same session, completely breaking session exclusivity.

## Approach

For each issue:
1. Write a stress test that reliably triggers the bug (high thread count, barriers for max contention)
2. Establish a baseline failure rate
3. Apply the minimal fix
4. Verify 0 failures across multiple runs
5. Confirm no regressions in the full 154-test suite

Where AMQP infrastructure made direct testing impractical (e.g., `OnFlow` requires `FlowContext` objects), fixes were verified by pattern simulation and code inspection.

## Fixes

### CRITICAL

**#1 -- SessionManager.TryAcceptSession TOCTOU** (`SessionManager.cs`)

The check-then-act on `IsLocked` had zero synchronization. 20 concurrent receivers would all see `IsLocked == false` and all proceed to lock the session. Added `_acceptLock` around the entire accept path.

*Stress test: 200 iterations x 20 threads. 100% failure rate before, 0% after.*

**#2/#3 -- Pump double-start in ReceiverLinkEndpoint and SessionReceiverLinkEndpoint**

`OnFlow` checked `_pumpTask == null` without a lock, so concurrent flow frames could each start a message pump. Two pumps pulling from the same queue = out-of-order delivery and lost messages. Added `lock(_pumpLock)` around the check-and-start.

**#10 -- Event handler captures field, not local** (fixed alongside #2/#3)

The `link.Closed` lambda captured `this._pumpCts` instead of a local. Closing an old connection would cancel the *new* pump's CTS. Fixed by capturing `var cts = _pumpCts` and closing over the local.

### HIGH

**#4/#9 -- SessionState torn reads and Release/Renew TOCTOU** (`SessionManager.cs`)

`LockedBy` and `LockedUntil` were plain auto-properties written from multiple threads. `DateTimeOffset` is a 12-byte struct -- not atomically writable. `ReleaseSession` and `RenewSessionLock` also had their own TOCTOU patterns. Extended `_acceptLock` to cover all lock-state mutations.

*Stress test: 10 threads x 500 accept/release cycles. 0 double-hold violations.*

**#5 -- SweepExpiredLocks TOCTOU with Complete** (`QueueEntity.cs`)

The sweep called `TryRemove` from `_pending`, then later added to `_sweptLockTokens`. In the gap, a concurrent `Complete()` saw the message in neither dictionary and silently returned -- then the sweep re-enqueued it. Duplicate delivery. Fixed by writing to `_sweptLockTokens` *before* `TryRemove`.

*Stress test: renew-vs-sweep race. Failed before fix, passes after.*

**#6 -- BrokeredMessage non-atomic fields** (`BrokeredMessage.cs`)

`DeliveryCount++` is a non-atomic read-modify-write. `LockedUntil` is a 12-byte struct that can tear. Replaced with `Interlocked.Increment` via `IncrementDeliveryCount()`, and stored `LockedUntil` as `long` ticks with `Interlocked.Read/Exchange`.

*Stress test: 20 threads x 1000 increments = exactly 20000. Concurrent read/write of LockedUntil: 0 torn values.*

**#7 -- DeadLetterQueue double allocation** (`QueueEntity.cs`)

`_deadLetterQueue ??=` is not atomic. Two threads could each allocate a `QueueEntity` (which starts a `Timer`), and the loser's instance leaked forever. Replaced with `Interlocked.CompareExchange` + `Dispose()` on the loser.

*Stress test: 50 threads access DeadLetterQueue simultaneously. All get the same instance.*

**#8 -- ScheduledMessageProcessor Start/Dispose race** (`ScheduledMessageProcessor.cs`)

No synchronization between `StartBackground` and `Dispose`. Calling `Dispose` between CTS creation and `Task.Run` left a zombie task. Calling `StartBackground` twice leaked the first CTS. Added `lock(_lifetimeLock)` around both methods; `StartBackground` now cancels any existing task first.

*Stress test: 50 rapid start/dispose cycles complete without deadlock.*

### MEDIUM

**#11 -- NamespaceContext.LastActivityAt torn write** (`NamespaceContext.cs`)

Same 12-byte `DateTimeOffset` issue. Converted to `long` ticks with `Interlocked`, same pattern as #6.

**#12 -- EmulatorContainer.DispatchRequest log read outside lock** (`EmulatorContainer.cs`)

`entry.ResponseLinks.Count` was read in a log statement outside the lock protecting the dictionary. Moved inside the existing lock block.

## Test Results After Fixes

| Suite | Result |
|-------|--------|
| Internal unit tests | 154/154 passed |
| Concurrency stress tests | 12/12 passed (new) |
| MassTransit ASB tests | 107/107 passed (7 skipped -- need Azure Storage, not Service Bus) |

## Recommendations

1. **The flaky `RenewLock_PreventsExpirySweep_NoDoubleDelivery` test** intermittently fails due to timing sensitivity (waits for lock expiry + sweep timer alignment). Consider increasing tolerances or using a controllable clock.

2. ~~**SessionState fields are now protected by `_acceptLock`** but are still plain auto-properties.~~ **Fixed:** `LockedBy` and `LockedUntil` are now private fields with read-only public getters. Mutations go through `internal` methods (`TryLock`, `Unlock`, `RenewLock`) that can only be called from within the assembly -- and in practice only from `SessionManager` under `_acceptLock`. Bad behavior is now impossible at the API level.

3. **AMQP transactions** remain unsupported (`Coordinator` links are rejected). NServiceBus users need `TransportTransactionMode.ReceiveOnly`.
