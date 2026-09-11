<img width="2013" height="1421" alt="image" src="https://github.com/user-attachments/assets/65a33f77-94c3-4ff8-8124-9172c5778fdc" />


# AlmostServiceBus

A local Azure Service Bus emulator compatible with the official Azure SDKs for .NET (`Azure.Messaging.ServiceBus`), Python, Node.js and Java, and with MassTransit, Wolverine, and NServiceBus. Every SDK and framework listed is exercised against the emulator in CI — see [Compatibility](#compatibility).

AlmostServiceBus is flexible in how you run it:

- **Embedded in your test suite** — runs in-process with per-test namespace isolation, so your integration tests run in parallel without interfering with each other
- **Standalone for local dev** — run as a dotnet tool or via Aspire, and point your app at it, just like the real thing
- **In a container** — ships with a Dockerfile; run it standalone or alongside your app in Docker Compose, with automatic container-host detection

## Features

- **No required infrastructure** — runs in-process or as a CLI tool with no SQL Server and no container needed (Docker is available if you want it)
- **Namespace isolation** —  `SharedAccessKeyName` can be passed an arbitrary value which will create an isolated namespace.
- **Full AMQP 1.0 protocol** via AMQPNetLite — no HTTP polling or fakes
- **Queues** with PeekLock, dead-lettering, duplicate detection, and max delivery count
- **Topics & Subscriptions** with SQL and correlation filters, forwarding, fan-out
- **Sessions** (FIFO) with session locking, next-available-session, isolated delivery, and session state
- **Scheduled messages** with enqueue-time semantics
- **AMQP transactions** — `System.Transactions.TransactionScope` works end-to-end, including cross-entity transactions (`EnableCrossEntityTransactions`); commit applies all operations atomically, rollback applies none
- **Batch message support** — correctly decodes Azure SDK `ServiceBusMessageBatch` transfers
- **Management API** — Atom XML REST API for queue/topic/subscription CRUD
- **Plaintext, MS-emulator compatible** — clients connect with `UseDevelopmentEmulator=true`, the same flag used with Microsoft's official Service Bus emulator. No TLS or dev cert needed for .NET. For the Node.js/Java/Python admin clients that require HTTPS, an optional TLS admin endpoint (port 5301) is provided with a generate-once CA or a certificate you supply.
- **Container-ready** — ships with a Dockerfile and auto-detects when it runs in a container: binds `0.0.0.0`, advertises its container hostname, and maps it (plus configurable aliases via `ASB_HOST`) to the `default` namespace
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
Endpoint=sb://localhost:5672;SharedAccessKeyName=<my-namespace>;SharedAccessKey=emulator;UseDevelopmentEmulator=true
```
*Note: using "RootManageSharedAccessKey" as the SharedAccessKeyName will map to the 'default' namespace.*

### Run with Docker

The emulator ships as a container image. Build it with the helper script and run it, exposing the
AMQP (5672), admin HTTP (5300), admin HTTPS (5301), and dashboard (15672) ports:

```bash
./docker/build-docker.sh              # builds almostservicebus:local
docker run --rm -p 5672:5672 -p 5300:5300 -p 5301:5301 -p 15672:15672 almostservicebus:local
```

From the host, connect to `localhost:5672` exactly as with the standalone tool. The emulator also
auto-detects when it runs in a container (binds `0.0.0.0`, advertises its container hostname), so
another container on the same network can reach it by service name.

See [`docker/README.md`](docker/README.md) for build details, prerequisites, Docker Compose, and
configuration (`ASB_HOST` and friends).

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
- **Performance/load testing** — this is an in-memory emulator, not a distributed broker. Backpressure and throughput characteristics don't match real ASB.
- **Production** — this should go without saying, but: don't.

**Framework coverage:** MassTransit has the deepest testing (runs against MassTransit's own ASB test suite). Wolverine and NServiceBus have conformance-level coverage. Contributions for additional framework tests, fixes, and new framework support are welcome.


## Architecture

```
Client (Azure SDK / MassTransit / Wolverine / NServiceBus)
UseDevelopmentEmulator=true in the connection string
    │
    ▼
TcpMultiplexer (port 5672, plaintext) ─── first-byte sniffing
    ├── 0x41 (AMQP)  → plain AMQP backend
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

The emulator is plaintext-only and compatible with Microsoft's official
Service Bus emulator: clients use the same `UseDevelopmentEmulator=true`
connection-string flag, which makes `Azure.Messaging.ServiceBus` use plain
AMQP for data and plain HTTP for admin.

The emulator uses AMQPNetLite as the AMQP server (Microsoft.Azure.Amqp's server API is internal). A custom `EmulatorContainer` (replacing AMQPNetLite's `ContainerHost`) handles delivery tag rewriting, batch message decoding, and transaction coordinator links (a server-side coordinator buffers transactional work and applies it on commit). Message delivery uses channel-based waiting for instant wake-up on enqueue.

## Compatibility

Everything in these tables runs against the emulator in CI on every commit, so the claims below are only as strong as the linked suites.

### Client SDKs

| SDK | Data plane | Management plane | Verified by |
|-----|------------|------------------|-------------|
| .NET `Azure.Messaging.ServiceBus` | **Full** — PeekLock, sessions, scheduled messages, processors, batch sends, transactions | **Full** — `ServiceBusAdministrationClient` honours `UseDevelopmentEmulator=true` (plain HTTP on 5300) | Internal suites + the framework suites below |
| Python `azure-servicebus` (pyamqp) | **Smoke-tested** — send/receive, properties, peek, lock renewal, abandon, dead-letter, sessions + state, topics, scheduled | REST only¹ | [`tests/client-sdk-smoke/python`](tests/client-sdk-smoke) |
| Node.js `@azure/service-bus` (rhea) | **Smoke-tested** — same scenario | REST only¹ | [`tests/client-sdk-smoke/node`](tests/client-sdk-smoke) |
| Java `azure-messaging-servicebus` (proton-j) | **Smoke-tested** — same scenario | REST only¹ | [`tests/client-sdk-smoke/java`](tests/client-sdk-smoke) |

¹ The Python, Node.js and Java admin clients only speak HTTPS. The emulator (like Microsoft's) is plaintext on the data plane, but also exposes an **HTTPS admin endpoint on port 5301** for exactly these clients — see [HTTPS admin endpoint](#https-admin-endpoint-nodejs-java-python). Point the admin client at `https://localhost:5301` and trust the generated (or your own) CA. You can also create entities through the Atom XML REST API on port 5300 directly — the smoke tests show how — or from .NET. The Atom XML the emulator emits is verified to parse in the Java and Node.js SDKs.

### Frameworks

| Framework | Status | Notes |
|-----------|--------|-------|
| MassTransit | **Full** | Tested against MassTransit's own ASB test suite |
| Wolverine | **High** | 149/155 tests pass; tracking correlation edge cases excluded |
| NServiceBus | **Partial** | AMQP transactions now supported (no longer requires `ReceiveOnly` transport mode) |

## Test Results

| Suite | Passed | Total |
|-------|--------|-------|
| Internal unit + integration | 207 | 207 |
| Conformance (emulator) | 34 | 34 |
| MassTransit ASB test suite | 26 | 27 |
| Wolverine ASB test suite | 149 | 155 |

## Configuration

| CLI argument | Default | Description |
|-------------|---------|-------------|
| `--Port` | 5672 | Main public port (plain AMQP + plain HTTP, multiplexed) |
| `--DashboardPort` | 15672 | Vue dashboard port (0 to disable) |
| `--AdminTlsPort` | 5301 | HTTPS admin port for clients that hard-code TLS (Node.js/Java/Python). `0` disables it. |
| `--AdminTlsCertDir` | `<app>/certs` | Directory where the emulator writes/reads its TLS material (CA, server PFX, Java truststore). Mount a volume here to keep the CA stable across restarts. |
| `--AdminTlsCertPath` | — | Path to your own certificate: a PKCS#12/PFX file, or a PEM certificate (pair with `--AdminTlsKeyPath`). |
| `--AdminTlsKeyPath` | — | Path to a PEM private key, when `--AdminTlsCertPath` points at a PEM certificate. |
| `--AdminTlsCertPassword` | — | Password for the supplied PFX (file or base64). |
| `--AdminTlsCertBase64` | — | Base64-encoded PFX, for supplying a certificate via configuration/env var (see Docker). |
| `--AdminTlsHosts` | — | Extra comma/space-separated hostnames/IPs to add to the auto-generated certificate's SANs. |

Additional ports bound automatically:
- **5300** — admin HTTP, the port `Azure.Messaging.ServiceBus` uses for management when `UseDevelopmentEmulator=true`
- **5301** — admin HTTPS, for the Node.js/Java/Python admin clients that only speak TLS (see [HTTPS admin endpoint](#https-admin-endpoint-nodejs-java-python))

### Environment variables

Mostly useful when running in a container. All are optional.

| Variable | Default | Description |
|----------|---------|-------------|
| `ASB_HOST` | container hostname (in a container), else `localhost` | The host clients use to reach the emulator (for example a Docker service name). Advertised as the public host and treated as the `default` namespace, so `RootManageSharedAccessKey` resolves to `default`. |
| `ASB_BIND_HOST` | `0.0.0.0` (in a container), else `localhost` | Network interface the listener binds to. |
| `ASB_DEFAULT_NAMESPACE_HOSTS` | — | Extra comma/space-separated hostnames that should also map to the `default` namespace. |
| `ASB_RUNNING_IN_CONTAINER` | — | Set to `true` to force container mode when the standard `DOTNET_RUNNING_IN_CONTAINER` flag isn't present (for example on a non–.NET base image). |

`DOTNET_RUNNING_IN_CONTAINER=true` is set automatically by Microsoft's official .NET base images, so container mode is usually detected without any configuration. Loopback addresses (`localhost`, `127.0.0.1`, `0.0.0.0`, `::1`) and `host.docker.internal` always map to the `default` namespace.

## HTTPS admin endpoint (Node.js, Java, Python)

The data plane is always plain AMQP (`UseDevelopmentEmulator=true`), and the .NET
`ServiceBusAdministrationClient` uses plain HTTP on port 5300 — so .NET needs no certificates. The
Node.js, Java and Python **admin** clients only speak HTTPS, so the emulator also exposes an HTTPS
admin endpoint on **port 5301**.

On first start it generates a local development CA and server certificate (reused on every later
start, so you trust the CA **once**) and writes three files to the cert directory:

| File | Purpose |
|------|---------|
| `emulator-ca.crt` | PEM CA — for Node.js, Python and curl |
| `emulator-truststore.p12` | PKCS#12 truststore (password `changeit`) — for Java |
| `emulator-admin.pfx` | server certificate used by the emulator itself |

Point your admin client at `https://localhost:5301`, trust the CA, and create entities as usual. You
can also **bring your own certificate** (`--AdminTlsCertPath`, `--AdminTlsKeyPath`,
`--AdminTlsCertPassword`, or `--AdminTlsCertBase64`) or disable TLS with `--AdminTlsPort 0`.

**See [`certs/README.md`](certs/README.md)** for per-language setup (Node.js, Java, Python, curl),
bring-your-own-certificate instructions, and the full list of certificate options.

## Known Limitations

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
