# MS Emulator Comparison

Runs identical test scenarios against **both** Microsoft's official Azure Service
Bus emulator and our AlmostServiceBus emulator, then diffs the results to find
where we diverge.

## Prerequisites

- Docker & Docker Compose (for the MS emulator)
- .NET 10 SDK
- `socat` (for the Wolverine harness only)
- `jq`, `curl`, `fuser` (standard POSIX tooling)

## Harnesses

### `run-lock-renewal-comparison.sh` — Lock renewal compatibility

Exercises every lock-renewal path the Azure SDK uses:

| Scenario | Expected |
|----|----|
| `MessageLock_RenewExtendsExpiry` | success, LockedUntil advances |
| `MessageLock_RenewAfterComplete_ThrowsMessageLockLost` | ServiceBusException(MessageLockLost) |
| `MessageLock_RenewAfterAbandon_ThrowsMessageLockLost` | ServiceBusException(MessageLockLost) |
| `MessageLock_RenewAfterNaturalExpiry_ThrowsMessageLockLost` | ServiceBusException(MessageLockLost) |
| `MessageLock_RenewMultipleTimes_Succeeds` | success |
| `SessionLock_RenewExtendsExpiry` | success |
| `SessionLock_RenewAfterNaturalExpiry_ThrowsSessionLockLost` | ServiceBusException(SessionLockLost) |
| `SessionLock_RenewMultipleTimes_Succeeds` | success |
| `MessageLock_AutoRenewalSurvivesSlowConsumer` | success (SDK auto-renews across 25s processing) |

```bash
./run-lock-renewal-comparison.sh
```

The script runs the suite twice (once per emulator) and prints a side-by-side
table plus a divergence list. Exits non-zero if any test outcome differs.

Output lives in `comparison-results/` (trx XML + logs).

### `run-wolverine-against-ms-emulator.sh` — Wolverine test compatibility

Runs Wolverine's Azure Service Bus tests against the MS emulator to determine if
Wolverine-test failures are transport-specific or Wolverine bugs. See the
script for details.

## Why lock-renewal-comparison exists

Under Black Friday load on our emulator we saw `R-DUPE` warnings in MassTransit
and "session channel N cannot be found" errors on both the server and the SDK
side. Most of these trace back to lock-renewal corner cases: the emulator
replying with a fake success when the token is unknown, races between
`RenewLock` and `SweepExpiredLocks`, or detach-response frames arriving at a
session the client has already End'd.

Having a deterministic, side-by-side harness lets us catch regressions and
incrementally close every divergence until we're 1:1 with the official service.

## Adding new scenarios

Add a `[Fact]` to `LockRenewalComparison/LockRenewalTests.cs`. The tests use
the SDK exclusively and read their connection string from
`SBE_CONNECTION_STRING`, so no harness changes are needed.

The MS emulator only creates queues listed in
`LockRenewalComparison/Config.json`, so if you need new entities, add them
there too.
