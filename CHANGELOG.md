# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- **Dashboard API routes are one route per operation.** The dashboard's JSON
  API used catch-all routes (`/queues/{**path}`) that inspected the tail of the
  path to decide between `/messages`, `/deadletter` and `/properties`. Each
  operation is now its own route, with subscriptions addressed as
  `/topics/{topic}/subscriptions/{subscription}/...`. Entity names, which may
  contain slashes, travel percent-encoded as a single path segment
  (`orders%2Feu` rather than `orders/eu`). The dashboard was updated to match;
  scripts that call the API directly need the same encoding.

### Fixed- **Batch sends from the Node.js SDK lost all but a garbled first message.** A
  batch is one AMQP transfer whose body is a list of encoded messages. The
  emulator recognised it only when the envelope had no `Subject`, which holds
  for the .NET SDK but not for `@azure/service-bus`, whose `sendMessages(array)`
  copies the first message's properties onto the envelope. Batches are now
  detected by the transfer's message-format (`0x80013700`), which every SDK
  sets. The Node.js, Python and Java smoke tests each gained a subject-bearing
  batch step.

## [0.5.0] - 2026-09-10

### Added
- **Dashboard icons.** The sidebar, entity header, tabs and message rows use
  [lucide](https://lucide.dev) icons (`lucide-vue-next`) for queues, topics,
  subscriptions, dead-letter queues and message state instead of hand-picked
  text glyphs, and the count badges have tooltips. One `EntityIcon` component
  decides which icon stands for which kind of entity.
- **Queue Properties tab** in the dashboard, backed by a new
  `GET /api/dashboard/namespaces/{ns}/queues/{name}/properties` endpoint:
  lock duration, max delivery count, session and duplicate-detection settings,
  TTL, auto-delete, forwarding, user metadata, and live counts.
- **Dead-letter reason and description** are returned by the dashboard API
  (`deadLetterReason`, `deadLetterErrorDescription`, `deadLetterSource`) and
  shown on dead-lettered rows and in the message detail pane.
- `Enqueued` dashboard events now carry the message's application properties,
  subject and correlation id, so rows that appear live show them without a
  refresh.
- **Node.js and Java SDK management-plane compatibility.** Atom XML entity
  descriptions now always carry `EnablePartitioning`, `EnableExpress`,
  `RequiresDuplicateDetection`, `EnableSubscriptionPartitioning` and
  `SupportOrdering`, entries include an Atom `<id>` and self `<link>`, and error
  payloads use a numeric `<Code>` — all of which those SDKs require when
  parsing. Contributed by @alt-Rational (#101).
- **Client SDK smoke tests in CI** (`tests/client-sdk-smoke`). The official
  Python (`azure-servicebus`), Node.js (`@azure/service-bus`) and Java
  (`azure-messaging-servicebus`) SDKs now run an end-to-end scenario against
  the emulator on every build: send/receive with application properties, peek,
  lock renewal, abandon, dead-letter and DLQ receive, sessions with session
  state, topic → subscription, scheduled messages. Entities are created over
  the plain-HTTP Atom API because only the .NET SDK's admin client supports a
  plaintext endpoint. Writing them surfaced the five protocol fixes below.

### Fixed
- **Dashboard purge endpoints now actually remove messages.** `DELETE .../messages`
  and `DELETE .../deadletter` only locked the messages, so they reappeared with a
  higher delivery count once the lock expired. They are now completed.
- **Node.js connections hung for ~60 s when several were opened at once.**
  AMQPNetLite's listener pipelines its AMQP header and `open` straight after
  the `sasl-outcome`; rhea (the Node transport) stops parsing at the
  `sasl-outcome` and parks the rest of that TCP chunk until the next socket
  data event, which never comes. The emulator's proxy now withholds the
  server's AMQP header until the client's header has been forwarded, so it
  always lands in a later chunk. Diagnosed, with a regression test, by
  @alt-Rational (#109); the equivalent listener-side change is proposed
  upstream in Azure/amqpnetlite#651.
- **Settlement replies now echo the client's outcome type** (`Modified` for
  abandon/defer, bare `Rejected` for dead-letter, `Accepted` for complete)
  instead of always `Accepted`. The Java SDK fails an abandon or dead-letter
  whose reply type differs from its request; the .NET SDK does not check the
  type but treats a `Rejected` reply *with an error* as a refused settlement,
  so the dead-letter reply must not echo the client's error map.
- **Dead-letter maps with string keys** (as sent by the Node.js SDK's rhea
  transport) no longer throw inside the disposition handler; previously the
  settlement was never sent and `deadLetterMessage` timed out.
- **A restated Flow no longer zeroes link credit.** The Python SDK re-sends an
  identical Flow on each `receive_messages` call; AMQPNetLite treats the zero
  delta as a credit reduction, so the receiver waited forever for a message
  that was in the queue.
- **Sender links are registered by entity path.** The Python SDK attaches its
  sender to `amqps://host:port/queue`; a scheduled message resolved through
  the sender-link registry was routed to an entity literally named that.
- **Request-processor reply links are keyed per connection.** The Java SDK uses
  the same reply-to (`cbs-client-reply-to`) on every connection; closing one
  connection removed the reply link another connection still needed and its
  next CBS token refresh — and the `scheduleMessage` waiting on it — hung.
- **Explicitly dead-lettered messages were invisible.** `DeadLetterMessageAsync`
  stamped the shared message object as dead-lettered before enqueuing it in the
  DLQ, so the dashboard's Dead Letter tab and the SDK's peek of the dead-letter
  sub-queue (`SubQueue.DeadLetter`) both filtered it out. Receiving from the DLQ
  still worked, which made the count and the list disagree. The source queue's
  history now keeps a snapshot and the DLQ copy stays active.
- Messages dead-lettered by exceeding `MaxDeliveryCount` were left in the source
  queue's live view (dashboard and SDK peek) as well as appearing in the DLQ.
- Application properties showed as "No application properties" on dashboard
  rows created from the live stream until the entity was re-selected.
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
