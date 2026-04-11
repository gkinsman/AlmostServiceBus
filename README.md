<img width="2013" height="1421" alt="image" src="https://github.com/user-attachments/assets/65a33f77-94c3-4ff8-8124-9172c5778fdc" />


# AlmostServiceBus

A local Azure Service Bus emulator compatible with the official Azure SDK (`Azure.Messaging.ServiceBus`), MassTransit, Wolverine, and NServiceBus.

AlmostServiceBus is flexible in how you run it:

- **Embedded in your test suite** — runs in-process with per-test namespace isolation, so your integration tests run in parallel without interfering with each other
- **Standalone for local dev** — run as a dotnet tool or via Aspire, and point your app at it, just like the real thing

## Features

- **No infrastructure dependencies** — no Docker, no SQL Server, no port conflicts
- **Namespace isolation** —  `SharedAccessKeyName` can be passed an arbitrary value which will create an isolated namespace.
- **Full AMQP 1.0 protocol** via AMQPNetLite — no HTTP polling or fakes
- **Queues** with PeekLock, dead-lettering, duplicate detection, and max delivery count
- **Topics & Subscriptions** with SQL and correlation filters, forwarding, fan-out
- **Sessions** (FIFO) with session locking, next-available-session, isolated delivery, and session state
- **Scheduled messages** with enqueue-time semantics
- **Batch message support** — correctly decodes Azure SDK `ServiceBusMessageBatch` transfers
- **Management API** — Atom XML REST API for queue/topic/subscription CRUD
- **TLS termination** — single port serves AMQPS, HTTPS, and plain AMQP/HTTP
- **Vue diagnostic dashboard** on port 15672


## Installation

Available as NuGet packages:

```bash
# CLI tool — quickest way to get started
dotnet tool install --global AlmostServiceBus.Tool

# Core emulator (standalone or embedded)
dotnet add package AlmostServiceBus

# Test host with per-test namespace isolation
dotnet add package AlmostServiceBus.TestHost

# Aspire integration
dotnet add package AlmostServiceBus.Aspire.Hosting
```

## Quick Start

### Run standalone

```bash
# If installed as a global tool:
almost-servicebus
```

Connection string:
```
Endpoint=sb://localhost:5672;SharedAccessKeyName=<my-namespace>;SharedAccessKey=emulator
```
*Note: using "RootManageSharedAccessKey" as the SharedAccessKeyName will map to the 'default' namespace.*

### Integration tests (in-process)

Add `AlmostServiceBus.TestHost` to your test project and use `ServiceBusEmulatorFixture`:

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

Add `AlmostServiceBus.Aspire.Hosting` to your Aspire host project:

```csharp
var builder = DistributedApplication.CreateBuilder(args);
var serviceBus = builder.AddServiceBusEmulator("servicebus");
```

## When to Use (and When Not To)

**Good fit:**
- **Integration tests** — per-test namespace isolation means your tests run in parallel without interfering. No Docker, no SQL Server, sub-second startup.
- **Local development** — `almost-servicebus` and go. Point your app at `localhost:5672` and iterate without an Azure subscription.
- **CI pipelines** — no Docker-in-Docker, no container orchestration. Just `dotnet tool install` and run.
- **Aspire apps** — drop-in `AddServiceBusEmulator()` resource, works like the real thing.

**Not a good fit:**
- **AMQP transactions** — Coordinator links are rejected. NServiceBus users need `TransportTransactionMode.ReceiveOnly`.
- **Performance/load testing** — this is an in-memory emulator, not a distributed broker. Backpressure and throughput characteristics don't match real ASB.
- **Production** — this should go without saying, but: don't.

**Framework coverage:** MassTransit has the deepest testing (runs against MassTransit's own ASB test suite). Wolverine and NServiceBus have conformance-level coverage. Contributions for additional framework tests, fixes, and new framework support are welcome.


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
    ├── AMQPNetLite + EmulatorContainer (message send/receive)
    └── Kestrel (management API + dashboard)
    │
    ▼
NamespaceRegistry (shared in-memory broker)
    ├── QueueEntity (channels, pending locks, DLQ)
    ├── TopicEntity → SubscriptionEntity (filters, forwarding)
    └── SessionManager (per-queue session partitioning)
```

The emulator uses AMQPNetLite as the AMQP server (Microsoft.Azure.Amqp's server API is internal). A custom `EmulatorContainer` (replacing AMQPNetLite's `ContainerHost`) handles delivery tag rewriting, batch message decoding, and coordinator link rejection. Message delivery uses channel-based waiting for instant wake-up on enqueue.

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
| Internal unit + integration | 190 | 190 |
| Conformance (emulator) | 34 | 34 |
| MassTransit ASB test suite | 26 | 27 |
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
- **Wolverine tracking** — `tracking_correlation_id_on_everything` compliance tests time out. Standalone tests confirm correct AMQP behavior; the timeout is caused by Wolverine's internal handler pipeline, not the emulator. See `tests/ms-emulator-comparison/` for a harness to verify against Microsoft's official emulator.

## Development

```bash
# Run all tests
dotnet test AlmostServiceBus.sln --filter "FullyQualifiedName!~RealAsbConformanceTests"

# Run conformance tests against real Azure Service Bus
ASB_CONNECTION_STRING="Endpoint=sb://..." dotnet test tests/AlmostServiceBus.Conformance.Tests \
  --filter "FullyQualifiedName~RealAsbConformanceTests"

# Run Wolverine tests (emulator must be running on port 5673)
dotnet run --project src/AlmostServiceBus.Host -- --Port 5673 --DashboardPort 0 &
dotnet test external/wolverine/src/Transports/Azure/Wolverine.AzureServiceBus.Tests -f net10.0

# Compare against Microsoft's official emulator
cd tests/ms-emulator-comparison && ./run-wolverine-against-ms-emulator.sh
```

## License

See [LICENSE](LICENSE) for details.
