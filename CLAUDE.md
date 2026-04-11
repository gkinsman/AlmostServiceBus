# Azure Service Bus Emulator — Session Handoff

## What This Is

A from-scratch Azure Service Bus emulator that runs locally, compatible with the real Azure SDK (`Azure.Messaging.ServiceBus`), MassTransit, Wolverine, and NServiceBus. Includes a Vue diagnostic dashboard and Aspire integration.

## Architecture

```
Client (Azure SDK / MassTransit / etc.)
    |
Port 5672 (public) — also 5671 (AMQPS), 5300 (HTTP), 443 (HTTPS)
    |
TcpMultiplexer (first-byte sniffing)
    ├── 0x41 (AMQP) → plain AMQP backend
    ├── 0x16 (TLS) → SslStream termination → detect HTTP vs AMQP
    └── HTTP methods → plain HTTP backend
    |
┌───────────────────┐    ┌──────────────────┐
│ AMQPNetLite        │    │ Kestrel HTTP      │
│ ConnectionListener │    │ (management API)  │
│ + EmulatorContainer│    │ (dashboard API)   │
└───────────────────┘    └──────────────────┘
         |                        |
    NamespaceRegistry (shared in-memory broker)
```

### Key Components

- **TcpMultiplexer** (`src/.../Hosting/TcpMultiplexer.cs`) — single port serves TLS/HTTPS/AMQP/plain HTTP
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

## Known Gaps

- **AMQP Transactions** — `Coordinator` links are gracefully rejected (`amqp:not-implemented`). NServiceBus defaults to transactions; use `TransportTransactionMode.ReceiveOnly` as workaround.
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
3. **TLS termination in multiplexer** — single port serves everything. Kestrel runs plain HTTP internally.
4. **Channel-based message pump** — `TryDequeueImmediate` + `WaitToReadAsync` (channel reader). Wakes instantly when a message is enqueued or abandoned. The only micro-delay is 1ms when waiting for AMQP link credit from the client.
5. **Clone() doesn't copy LockToken or SequenceNumber** — each queue assigns fresh values on enqueue. Copying caused R-DUPE in MassTransit.
