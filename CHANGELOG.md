# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project follows
[Semantic Versioning](https://semver.org/).

## [0.4.0] - 2026-09-10

### Added
- **Python `azure-servicebus` client support.** The emulator now accepts the
  Python SDK's `pyamqp` transport: local `open`/`begin`/`attach` performatives
  carry their full field lists, management responses keep the type of the
  request's message-id (pyamqp sends uuids), and link addresses given as a full
  URI (`amqps://host/entity`) resolve to the entity path. Contributed by
  @dmitrymizernik (#80).
- **Aspire readiness health check.** The `servicebus` endpoint exposes a TCP
  readiness check so `WaitFor` blocks until the emulator actually accepts
  connections, not just until the process starts.
- **Dashboard connection string panel.** The sidebar shows a copyable
  connection string, backed by a new `/api/dashboard/info` endpoint that also
  reports the emulator's ports.

### Changed
- Updated dependencies, notably **Aspire.Hosting 9 → 13** (now 13.5.3).
  Consumers of `AlmostServiceBus.Aspire.Hosting` must move their Aspire stack
  to 13.
- AMQPNetLite 2.5.1 → 2.5.4, Azure.Messaging.ServiceBus → 7.20.2.
- Proxy sockets now use `TCP_NODELAY`, shaving Nagle/delayed-ACK latency off
  the many small round-trips in the AMQP handshake.
- Benign peer resets (health probes, port scans, mid-handshake disconnects)
  are logged at Debug instead of Warning.
- Console output is forced to UTF-8 so the startup banner renders correctly
  when stdout is captured (e.g. under Aspire).

### Fixed
- Emulator now rejects a receiver on a second top-level entity over a
  cross-entity-transaction connection, matching real Azure Service Bus
  ("Local transactions cannot span multiple top-level entities") (#33).
- **Aspire: non-default ports work.** `AddServiceBusEmulator` no longer passes
  `--no-launch-profile` to `dotnet exec`; the Host's command-line parser was
  swallowing the `--Port` value that followed it, so the emulator listened on
  5672 while the connection string advertised the requested port (#80).
- Messages with a non-string AMQP `message-id` or `correlation-id` (uuid,
  ulong, binary) no longer throw on receive; the id is preserved as text.

### Security
- Resolved a high-severity `MessagePack` advisory (pulled transitively via
  `Aspire.Hosting`).
- Dashboard build tooling: `vite` bumped to 6.4.3 for a security advisory
  (#53). Build-time only; not shipped in any package.

## [0.3.1] - 2026-06-03

Earlier releases are described in the GitHub release notes.
