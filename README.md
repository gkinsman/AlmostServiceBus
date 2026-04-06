# AlmostServiceBus

A local Azure Service Bus emulator compatible with the official Azure SDK (`Azure.Messaging.ServiceBus`), MassTransit, Wolverine, and NServiceBus.

Unlike Microsoft's official emulator (which runs as a Docker container with SQL Server and supports only sequential testing), AlmostServiceBus runs **in-process** with per-test namespace isolation — so your integration tests run in parallel without interfering with each other.

## Features

- **In-process test host** — no Docker, no SQL Server, no port conflicts between test runs
- **Namespace isolation** — each test fixture gets a unique namespace via `SharedAccessKeyName`, enabling safe parallel execution
- **Full AMQP 1.0 protocol** via AMQPNetLite — no HTTP polling or fakes
- **Queues** with PeekLock, dead-lettering, duplicate detection, and max delivery count
- **Topics & Subscriptions** with SQL and correlation filters, forwarding, fan-out
- **Sessions** (FIFO) with session locking, next-available-session, isolated delivery, and session state
- **Scheduled messages** with enqueue-time semantics
- **Batch message support** — correctly decodes Azure SDK `ServiceBusMessageBatch` transfers
- **Management API** — Atom XML REST API for queue/topic/subscription CRUD
- **TLS termination** — single port serves AMQPS, HTTPS, and plain AMQP/HTTP
- **Namespace isolation** — use `SharedAccessKeyName` as namespace for test isolation
- **Vue diagnostic dashboard** on port 15672

## Quick Start

### Run standalone

```bash
dotnet run --project src/AzureServiceBusEmulator.Host
```

Connection string:
```
Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator
```

### Integration tests (in-process)

Reference `AzureServiceBusEmulator.TestHost` and use `ServiceBusEmulatorFixture`:

```csharp
var fixture = new ServiceBusEmulatorFixture();
await fixture.StartAsync();

var client = new ServiceBusClient(fixture.ConnectionString, new ServiceBusClientOptions
{
    TransportType = ServiceBusTransportType.AmqpTcp,
    CustomEndpointAddress = new Uri($"sb://localhost:{fixture.PublicPort}")
});

// Use client normally...
await fixture.DisposeAsync();
```

Each fixture gets a unique namespace, so tests run in parallel without interference.

### Aspire integration

Reference `AzureServiceBusEmulator.Aspire.Hosting`:

```csharp
var builder = DistributedApplication.CreateBuilder(args);
var serviceBus = builder.AddAzureServiceBusEmulator("servicebus");
```

## Architecture

```
Client (Azure SDK / MassTransit / Wolverine / NServiceBus)
    │
    ▼
TcpMultiplexer (port 5672) ─── first-byte sniffing
    ├── 0x41 (AMQP)  → plain AMQP backend
    ├── 0x16 (TLS)   → SslStream → AMQP or HTTP
    └── HTTP verb     → plain HTTP backend
    │
    ├── AMQPNetLite ContainerHost (message send/receive)
    └── Kestrel (management API + dashboard)
    │
    ▼
NamespaceRegistry (shared in-memory broker)
    ├── QueueEntity (channels, pending locks, DLQ)
    ├── TopicEntity → SubscriptionEntity (filters, forwarding)
    └── SessionManager (per-queue session partitioning)
```

The emulator uses AMQPNetLite as the AMQP server (Microsoft.Azure.Amqp's server API is internal). A custom `IContainer` implementation handles delivery tag rewriting, batch message decoding, and coordinator link rejection.

## Framework Compatibility

| Framework | Status | Notes |
|-----------|--------|-------|
| Azure SDK (`Azure.Messaging.ServiceBus`) | **Full** | PeekLock, sessions, scheduled messages, processors, batch sends |
| MassTransit | **Full** | Tested against MassTransit's own ASB test suite |
| Wolverine | **High** | 149/155 tests pass; tracking correlation edge cases excluded |
| NServiceBus | **Partial** | Transactions not supported; use `ReceiveOnly` transport mode |

## Test Results

| Suite | Passed | Total |
|-------|--------|-------|
| Internal unit + integration | 206 | 206 |
| Conformance (vs real ASB) | 22 | 22 |
| MassTransit ASB test suite | 26 | 26 |
| Wolverine ASB test suite | 149 | 155 |

## Configuration

| CLI argument | Default | Description |
|-------------|---------|-------------|
| `--Port` | 5672 | Main public port (AMQPS + AMQP + HTTP) |
| `--DashboardPort` | 15672 | Vue dashboard port (0 to disable) |

Additional ports bound automatically:
- **5671** — dedicated AMQPS
- **5300** — management API (HTTP, for `UseDevelopmentEmulator=true` clients)
- **443** — HTTPS (if available)

## Known Limitations

- **AMQP Transactions** — `Coordinator` links are gracefully rejected (`amqp:not-implemented`). NServiceBus defaults to transactions; use `TransportTransactionMode.ReceiveOnly` as workaround.
- **Lock renewal** — Server-side logic works but entity-scoped management link responses for lock renewal may have framing issues under certain conditions.
- **Wolverine tracking** — `tracking_correlation_id_on_everything` compliance tests time out. Standalone tests confirm correct AMQP behavior; the timeout is caused by Wolverine's internal handler pipeline, not the emulator. See `tests/ms-emulator-comparison/` for a harness to verify against Microsoft's official emulator.

## Development

```bash
# Run all tests
dotnet test AzureServiceBusEmulator.sln --filter "FullyQualifiedName!~RealAsbConformanceTests"

# Run conformance tests against real Azure Service Bus
ASB_CONNECTION_STRING="Endpoint=sb://..." dotnet test tests/AzureServiceBusEmulator.Conformance.Tests \
  --filter "FullyQualifiedName~RealAsbConformanceTests"

# Run Wolverine tests (emulator must be running on port 5673)
dotnet run --project src/AzureServiceBusEmulator.Host -- --Port 5673 --DashboardPort 0 &
dotnet test external/wolverine/src/Transports/Azure/Wolverine.AzureServiceBus.Tests -f net10.0

# Compare against Microsoft's official emulator
cd tests/ms-emulator-comparison && ./run-wolverine-against-ms-emulator.sh
```

## License

See [LICENSE](LICENSE) for details.
