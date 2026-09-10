# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- **Connection kills under sustained load.** A `ServiceBusSessionProcessor`
  with more session slots than sessions long-polls for "next available
  session". The SDK tells the service how long it will wait (the
  `com.microsoft:timeout` attach property) so the service answers first; the
  emulator ignored it and waited a fixed 65 s against the SDK's 60 s default.
  The client gave up, ended its AMQP session, and the emulator's late Attach
  landed on a channel that no longer existed — Microsoft.Azure.Amqp then closed
  the whole connection ("The session channel 'N' cannot be found") and every
  link on it went down. In MassTransit this surfaced as bursts of `R-DUPE`
  and `T-FAULT` across unrelated queues every ~60 s. The emulator now honours
  the client's timeout (capped at 65 s like the real service) and never sends
  a frame on a pending session attach once the client has closed the link.
- **Messages in flight were redelivered when a receive link dropped.** When a
  link is aborted (detach, session end, connection loss) AMQPNetLite
  synthesises a `Released` outcome for every unsettled delivery; the emulator
  treated these as client abandons and re-enqueued everything the consumer
  had prefetched or was still processing, producing duplicate deliveries and
  premature dead-lettering. Real Service Bus keeps the lock until it expires.
  Teardown outcomes are now ignored; for session queues the messages are
  reclaimed when a receiver next accepts the session.
- **Session queue `MessageCount` never went down.** Session dequeues bypassed
  the queue's counter, so the dashboard showed a session queue growing forever
  even while it was being drained.
- Settled messages are no longer kept in memory indefinitely; each queue keeps
  a bounded history (200) for the dashboard. `TotalMessageCount` /
  `ConsumedCount` are plain counters instead of scans.
- Removed a per-message `Console.Error` write on session-queue enqueue.
- OrderFlow demo: the fulfillment worker now has a retry policy, so the
  `ShipOrderConsumer`'s "will retry" is true and Black Friday runs drain
  completely instead of parking ~2% of orders in `logistics-dispatch_error`.
- OrderFlow demo dashboard: the browser tab no longer freezes under Black
  Friday load. Counters and pipeline state are polled from the API once a
  second (and resynced when the SSE stream reconnects) instead of being
  counted from events, so dropped bursts no longer skew the numbers; the
  throughput chart uses a fixed 60-bucket ring instead of re-filtering every
  event; SSE events are applied on a 250 ms tick with chart animation off; the
  server batches SSE writes, sends keep-alives and buffers 4096 events per
  subscriber.

## [0.4.0] - 2026-09-10

### Added
- **Python `azure-servicebus` client support.** The emulator now accepts the
  Python SDK's `pyamqp` transport: local `open`/`begin`/`attach` performatives
  carry their full field lists, management responses keep the type of the
  request's message-id (pyamqp sends uuids), and link addresses given as a full
  URI (`amqps://host/entity`) resolve to the entity path. Diagnosed and
  contributed by @dmitrymizernik (#79, #80).
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
  5672 while the connection string advertised the requested port. Found and
  fixed by @dmitrymizernik (#80).
- Messages with a non-string AMQP `message-id` or `correlation-id` (uuid,
  ulong, binary) no longer throw on receive; the id is preserved as text.
  Fixed by @dmitrymizernik (#80).

### Security
- Resolved a high-severity `MessagePack` advisory (pulled transitively via
  `Aspire.Hosting`).
- Dashboard build tooling: `vite` bumped to 6.4.3 for a security advisory
  (#53). Build-time only; not shipped in any package.

## [0.3.1] - 2026-06-03

Earlier releases are described in the GitHub release notes.
