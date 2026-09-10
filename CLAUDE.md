# Azure Service Bus Emulator — Session Handoff

## What This Is

A from-scratch Azure Service Bus emulator that runs locally, compatible with the real Azure SDK (`Azure.Messaging.ServiceBus`), MassTransit, Wolverine, and NServiceBus. Includes a Vue diagnostic dashboard and Aspire integration.

## Architecture

```
Client (Azure SDK / MassTransit / etc.) — UseDevelopmentEmulator=true
    |
Port 5672 (public AMQP/HTTP), Port 5300 (admin HTTP, MS-emulator compat)
    |
TcpMultiplexer (first-byte sniffing — plaintext only)
    ├── 0x41 (AMQP)    → plain AMQP backend
    └── HTTP verb byte → plain HTTP backend
    |
┌───────────────────┐    ┌──────────────────┐
│ AMQPNetLite        │    │ Kestrel HTTP      │
│ ConnectionListener │    │ (management API)  │
│ + EmulatorContainer│    │ (dashboard API)   │
└───────────────────┘    └──────────────────┘
         |                        |
    NamespaceRegistry (shared in-memory broker)
```

The emulator is plaintext-only — it matches Microsoft's official Service Bus
emulator, which clients reach via `UseDevelopmentEmulator=true` in the
connection string. That flag tells `Azure.Messaging.ServiceBus` to use plain
AMQP for data and plain HTTP for admin. No TLS, no certificates, no
privileged port binding.

### Key Components

- **TcpMultiplexer** (`src/.../Hosting/TcpMultiplexer.cs`) — single port serves plain AMQP and plain HTTP, routed by first-byte sniffing
- **EmulatorContainer** (`src/.../Amqp/EmulatorContainer.cs`) — custom `IContainer` replacing AMQPNetLite's `ContainerHost` (fixes transaction coordinator crash)
- **ReceiverLinkEndpoint** (`src/.../Amqp/ReceiverLinkEndpoint.cs`) — message pump with channel-based delivery (`WaitToReadAsync`), AMQP drain support, credit checking via reflection
- **SessionReceiverLinkEndpoint** (`src/.../Amqp/SessionReceiverLinkEndpoint.cs`) — session-aware variant
- **GuidDeliveryTagHandler** (`src/.../Amqp/GuidDeliveryTagHandler.cs`) — `IHandler` that rewrites 4-byte delivery tags to 16-byte GUIDs (Azure SDK requires this for PeekLock)
- **SessionManager** (`src/.../Broker/SessionManager.cs`) — per-queue session partitioning
- **ManagementLinkEndpoint** (`src/.../Amqp/ManagementLinkEndpoint.cs`) — handles `$management` AMQP operations
- **ManagementApiEndpoints** (`src/.../Management/ManagementApiEndpoints.cs`) — Atom XML REST API with catch-all routes for slashed entity names
- **Dashboard** (`src/AlmostServiceBus.Dashboard/`) — Vue 3 app on port 15672

### Namespace Isolation

Namespace is extracted from `SharedAccessKeyName` in the connection string (NOT the hostname). `RootManageSharedAccessKey` maps to `"default"` namespace. Any other key name becomes the namespace. This allows GUIDs as namespace identifiers for test isolation.

## Test Results (224 internal + frameworks)

| Suite | Passed | Total |
|-------|--------|-------|
| Emulator internal | 154 | 154 |
| SDK integration | 36 | 36 |
| Conformance (emulator) | 34 | 34 |
| MassTransit own ASB tests | 26 | 27 |
| Wolverine ASB tests | 149 | 155 |
| NServiceBus emulator tests | 2 | 3 |

## Sessions

### What works
- Session send/receive with FIFO ordering ✅
- Multiple sessions with isolated delivery ✅
- Session locking (one receiver per session) ✅
- Session filter detection on AMQP receiver links ✅
- Next-available-session (`AcceptNextSessionAsync`) ✅ — session ID returned via Source filter-set in AMQP attach response
- Wolverine session tests (3/3 pass) ✅

- `SetSessionStateAsync` / `GetSessionStateAsync` ✅ — now working

## AMQP Batch Messages

Azure SDK's `ServiceBusMessageBatch` sends messages as a single AMQP transfer where the body contains `Data[]` sections, each being a complete AMQP-encoded inner message. The emulator detects this format and decodes individual messages, preserving all properties (Subject, ApplicationProperties, etc.). Verified by 15 dedicated integration tests covering batch+processor, plain AMQP, two-client, and cascading-send scenarios.

## AMQP Transactions

Supported. The emulator runs a server-side transaction coordinator so the Azure
SDK's `TransactionScope` works end-to-end, including cross-entity transactions
(`ServiceBusClientOptions.EnableCrossEntityTransactions = true`).

- **TransactionManager** (`src/.../Broker/Transactions/TransactionManager.cs`) — broker-agnostic. `Declare()` issues a GUID `txn-id`; callers `Enlist` commit/rollback delegates; `Commit` applies them in order, `Rollback` discards. Globally-unique ids mean one flat table spans all connections and entities.
- **TransactionCoordinatorEndpoint** (`src/.../Amqp/TransactionCoordinatorEndpoint.cs`) — accepts the `Coordinator` link (previously rejected) and services `declare`/`discharge`, replying with `Declared`/`Accepted`.
- **Buffering** — `SenderLinkEndpoint` buffers transactional sends (delivery-state is `TransactionalState`), the receiver endpoints buffer transactional settlements. Both echo a transactional disposition; the work applies on commit. On rollback, sends are discarded and uncommitted completes simply let the lock expire (redelivery bumps `DeliveryCount`).
- Verified by `TransactionManagerTests` (unit) and `TransactionTests` (real Azure SDK: commit/rollback, single- and cross-entity).

## Known Gaps

- **Wolverine tracking** — `tracking_correlation_id_on_everything` compliance tests time out. Standalone tests confirm correct AMQP behavior; the timeout is in Wolverine's handler pipeline. See `tests/ms-emulator-comparison/` to verify against Microsoft's official emulator.

## Running

```bash
# Start emulator
cd src/AlmostServiceBus.Host && dotnet run

# Run all tests
dotnet test AlmostServiceBus.sln --filter "FullyQualifiedName!~RealAsbConformanceTests" --verbosity quiet

# Run against real ASB for comparison
ASB_CONNECTION_STRING="Endpoint=sb://..." dotnet test tests/AlmostServiceBus.Conformance.Tests --filter "FullyQualifiedName~RealAsbConformanceTests"

# Run external framework tests (emulator must be running)
cd external/wolverine && dotnet test src/Transports/Azure/Wolverine.AzureServiceBus.Tests --no-build -f net9.0
cd external/MassTransit && MT_ASB_EMULATOR=1 MT_ASB_KEYNAME=RootManageSharedAccessKey MT_ASB_KEYVALUE=emulator dotnet test tests/MassTransit.Azure.ServiceBus.Core.Tests --no-build
```

## Versioning & Releasing

Versioning is fully automatic via [MinVer](https://github.com/clcrutch/MinVer). No version numbers in csproj files.

- MinVer reads the version from **git tags** (prefix `v`, e.g. `v0.1.0`)
- On a tagged commit: version is exactly the tag (e.g. `0.1.0`)
- After a tag: version auto-bumps patch with pre-release suffix (e.g. `0.1.1-alpha.0.3` = 3 commits after `v0.1.0`)
- No tags at all: version is `0.1.0-alpha.0.N` (floor set by `MinVerMinimumMajorMinor` in `Directory.Build.props`)

### How to release

```bash
git tag v0.1.0
git push origin v0.1.0
```

The `.github/workflows/ci.yml` workflow triggers on `v*` tags and:
1. Runs the normal test matrix once
2. Packs three NuGet packages: `AlmostServiceBus`, `AlmostServiceBus.TestHost`, `AlmostServiceBus.Aspire.Hosting`
3. Pushes to NuGet.org (requires `NUGET_USER` for OIDC login)

### Version bumps

- **Patch**: automatic (MinVer bumps patch between tags)
- **Minor**: create tag `v0.2.0`
- **Major**: create tag `v1.0.0`

No csproj changes needed — just tag and push.

## Key Design Decisions

1. **AMQPNetLite not Microsoft.Azure.Amqp** — Microsoft.Azure.Amqp's server-side API is internal/undocumented. AMQPNetLite works but needs workarounds (delivery tags, credit reflection).
2. **Custom IContainer** — replaced `ContainerHost` to handle `Coordinator` targets without crashing.
3. **Plaintext-only, MS-emulator compatible** — no TLS, no dev cert, no privileged ports. Clients connect via `UseDevelopmentEmulator=true`, which makes the Azure SDK speak plain AMQP (port 5672) and plain HTTP (port 5300) — the same wire-level behaviour as Microsoft's official emulator.
4. **Channel-based message pump** — `TryDequeueImmediate` + `WaitToReadAsync` (channel reader). Wakes instantly when a message is enqueued or abandoned. The only micro-delay is 1ms when waiting for AMQP link credit from the client.
5. **Clone() doesn't copy LockToken or SequenceNumber** — each queue assigns fresh values on enqueue. Copying caused R-DUPE in MassTransit.
6. **Link teardown is not settlement** — when AMQPNetLite aborts a link (client detach, session end, connection loss) it synthesises `Released`/`Rejected` outcomes for every unsettled outgoing delivery. `ReceiverLinkEndpoint.IsTeardownOutcome` filters these out (the link is already `IsClosed` when they arrive). Treating them as abandons redelivered in-flight messages to the reconnected consumer (MassTransit `R-DUPE`). Like real ASB, a lost link leaves messages locked until `LockDuration` expires; session messages are reclaimed on the next session accept.
7. **Pending session accepts honour `com.microsoft:timeout`** — for next-available-session the SDK sends its wait budget in the Attach and expects the *service* to time out first. `ServiceBusLinkProcessor.PendingSessionAttach` waits at most that long (capped at 65s) and never sends a frame after the client has closed the link. Sending a late Attach on an ended AMQP session makes the SDK close the whole connection ("The session channel 'N' cannot be found"), taking every other link down with it.
8. **Settlement replies echo the client's outcome type** — `ReceiverLinkEndpoint.SettleWithClientOutcome` answers `Modified`→`Modified`, `Released`→`Released`, `Rejected`→bare `Rejected`, else `Accepted`. AMQPNetLite's `DispositionContext.Complete()` always says `Accepted`, which the Java SDK rejects as an outcome mismatch; a `Rejected` *with an error* is what the .NET SDK treats as a refused settlement, so the dead-letter reply must not carry the client's error map.
9. **Non-.NET clients are the conformance oracle for AMQP edge cases** — `tests/client-sdk-smoke` (Python/Node/Java, run in CI) found: string-keyed AMQP maps (enumerate `Map` entries, never index `Fields` by key), pyamqp restating an identical Flow (AMQPNetLite zeroes credit on a zero delta; `ReceiverLinkEndpoint.CreditShadow` restores it), sender links registered by raw URI address (normalise before the schedule-message registry), and the Java SDK reusing one reply-to address on every connection (request-processor reply links are keyed by connection + reply-to). When touching AMQP framing, run those three scripts, not just the .NET suites.
10. **Only the .NET admin client can reach a plaintext management endpoint** — the Python, Node and Java admin clients hard-code HTTPS. Don't try to make them work against port 5300; the smoke tests create entities over the REST API directly.
