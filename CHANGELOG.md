# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project follows
[Semantic Versioning](https://semver.org/).

## [0.7.2] - 2026-10-15

### Fixed
- **The emulator exits on SIGTERM.**
  - Aspire stops executable resources with SIGTERM on Linux and macOS, so
    when an AppHost shut down, the emulator it launched kept running,
    holding its ports and memory until it was killed by hand.
  - The Host only shut down on Ctrl+C or process exit. The console lifetime
    of its two web apps swallowed SIGTERM and only requested a stop, which
    nothing waited on.
  - The Host now also shuts down when either web app is asked to stop.

## [0.7.1] - 2026-10-13

### Changed
- **Dependency updates.** `System.Security.Cryptography.Pkcs` 10.0.13 (#171)
  and the dashboard's Vue 3.5.44 (#170). No behaviour changes.

## [0.7.0] - 2026-10-13

### Changed
These bring the emulator closer to a real namespace, but they can surface
errors that used to be hidden.

- **Sending to an entity that doesn't exist fails with
  `MessagingEntityNotFound`.**
  - This used to create a queue of that name, which hid mistakes such as
    publishing to a topic that was never created.
  - Receiving from, and scheduling to, a missing entity fail the same way.
  - Create entities before using them, through the admin client or, in tests,
    the TestHost `NamespaceContext`. (#161)
- **The emulator listens on loopback unless it runs in a container.**
  - Its public listeners used to bind every interface: the AMQP/HTTP port,
    `5300`, the dashboard and the TLS admin endpoint. On Windows each newly
    built Host or test binary then prompted for a firewall exception.
  - They now bind `ASB_BIND_HOST`, which defaults to `localhost` outside a
    container and `0.0.0.0` inside one, as the README already described.
  - Container mode is detected from `DOTNET_RUNNING_IN_CONTAINER`, which the
    official .NET images (including ours) set, or from
    `ASB_RUNNING_IN_CONTAINER`.
  - To accept connections from other machines, set `ASB_BIND_HOST=0.0.0.0`.
    (#163)
- **Scheduling a message without a session id to a session queue is refused**
  with `InvalidOperationException` when it is scheduled, as on a real
  namespace. (#162)
- **The Aspire integration gives containers a host they can reach.**
  - The connection string used to hard-code `localhost`, so a container
    resource referencing the emulator could never connect.
  - Each consumer now gets the emulator's per-consumer host: projects get
    `localhost`, and containers get `aspire.dev.internal`, through Aspire's
    container tunnel.
  - `aspire.dev.internal` maps to the default namespace. (#163)

### Fixed
- **Auto-forwarding to a topic.** A queue or subscription whose `ForwardTo`
  named a topic kept every message instead of forwarding it, and queue
  `ForwardTo` was ignored entirely. MassTransit builds its publish topology
  from subscription-to-topic forwards, so consumers of a base type or
  interface received nothing published as a derived type. `ForwardTo` now
  delivers to a queue or a topic, resolved by name on each delivery, so a
  destination created later or recreated is picked up. (#158, #159)
- **Auto-forwarding edge cases now match a real namespace**, checked against a
  Standard tier namespace rather than the documentation, which differs:
  - A message that can't be forwarded goes to the source's dead-letter queue,
    not the transfer dead-letter queue. This happens when the destination no
    longer exists, on a fifth forward, or when a message without a session id
    is forwarded to a session queue. The dead-letter reasons are the ones
    Service Bus sets, e.g. `Maximum transfer hop count is exceeded.`
  - Creating or updating a queue or subscription whose `ForwardTo`
    destination doesn't exist fails with `ArgumentException`.
  - Receiving from or peeking an entity with `ForwardTo` fails with
    `InvalidOperationException`. Its dead-letter queue is still readable.
  - Updating a subscription without `ForwardTo` now clears it.

  The dashboard shows a forwarding subscription's own message count rather
  than its destination's. MassTransit's `Publish_with_complex_hierarchy` test
  runs in CI again. (#159)
- **A topic with a session subscription no longer rejects messages without a
  session id.**
  - The publish used to throw, and the subscriptions after the session one got
    nothing.
  - As on a real namespace, the publish now succeeds, the other subscriptions
    get their copies, and the session subscription dead-letters its copy with
    `Session id is null.`
  - A direct send to a session queue is still refused. (#160)
- **One bad scheduled message no longer stops all scheduled delivery.**
  - A due message that couldn't be enqueued ended the background loop, so
    nothing scheduled was delivered again until restart, and that message was
    lost.
  - Each due message is now delivered on its own. One that fails, or whose
    target has been deleted, is logged and dropped. (#162)
- **Released messages come straight back.**
  - A receiver that hands a message back unprocessed (AMQP `released`) used to
    trigger the same 1 s delay and delivery-count increase as an abandon.
  - With idle receivers holding credit, as Wolverine leaves them, request-reply
    could then miss its timeout.
  - On non-session queues, released messages are now redelivered at once and
    don't count as a delivery. (#164)
- **Test-only:**
  - The NServiceBus emulator acceptance tests now run in CI. (#166)
  - 9 MassTransit and 15 Wolverine tests that used to be excluded run again.
    (#167)
  - Wolverine's `check_negative` smoke test is fixed, and the framework
    patches now apply on Windows checkouts. (#164)
  - Duplicate detection of scheduled and auto-forwarded messages is pinned to
    real Service Bus behaviour by new tests. (#165)

## [0.6.2] - 2026-09-30

### Added
- **Runtime message counts in the admin API.** Queue, topic and subscription
  entries now carry `MessageCount` and `CountDetails`, so
  `GetQueueRuntimePropertiesAsync`, `GetTopicRuntimePropertiesAsync`,
  `GetSubscriptionRuntimePropertiesAsync` and their paged listings report
  real `ActiveMessageCount` (locked messages included, as in Service Bus),
  `DeadLetterMessageCount` and `ScheduledMessageCount` instead of zeros.
  Topics also report `SubscriptionCount`. (#137)
- **Peeking scheduled messages on a topic.** Scheduled messages sent to a topic
  stay on the topic until they fire. A receiver created with the topic name can
  now peek them (`State == Scheduled`), which is the workaround Service Bus
  documents for them. (#137)

### Changed
- `AlmostServiceBus.Aspire.Hosting` now depends on `Aspire.Hosting` 13.6.0
  (#140).
- Dependency updates for the test suites and dashboard build (no change to the
  shipped packages): `Azure.Messaging.ServiceBus` 7.21.0 (#133),
  `Microsoft.NET.Test.Sdk` 18.10.1 in the Aspire tests (#127),
  `coverlet.collector` 10.1.0 (#136), `vite` 8.3.1 (#132), and for the Java
  smoke tests `azure-messaging-servicebus` 7.18.0 (#134) and `slf4j-simple`
  2.0.20 (#131); the `external/NServiceBus.Transport.AzureServiceBus`
  submodule to `e35643a` (#130).

### Fixed
- **Intermittent CI failures in the unit tests.** The concurrency tests parked
  up to 50 thread-pool threads at a `Barrier`, starving the pool for seconds
  and stalling timers in the lock-expiry and renewal tests running alongside;
  that blocking work now runs on dedicated threads. The multiplexer header
  tests wait for the bytes they expect instead of reading a fixed window, and
  `RenewLock_PreventsExpirySweep_NoDoubleDelivery` no longer fails whenever
  the sweep wins the race. Test-only. (#138, #139)

## [0.6.1] - 2026-09-18

### Added
- **HTTPS admin endpoint for Node.js, Python and Java (opt-in TLS).** The
  official `@azure/service-bus`, `azure-servicebus` and
  `azure-messaging-servicebus` `ServiceBusAdministrationClient`s always speak
  HTTPS, so they could not reach the plaintext-only management API and only
  the .NET admin client worked. The emulator can now serve the
  management API over TLS on a separate port, so those clients can create and
  manage entities against it. A self-signed CA and server certificate are
  generated and persisted on first start and reused on restart, or you can
  bring your own (PEM/PFX via path, or base64, with an optional password); the
  Aspire dev certificate is picked up automatically when running under Aspire.
  It is **off by default** — enable it with `AdminTlsEnabled=true`; the port is
  `AdminTlsPort` (default `5301`) and the certificate directory is
  `AdminTlsCertDir`. The AMQP data plane (`5672`) and the plaintext admin API
  (`5300`) are unchanged, so this is fully backward compatible. The Java admin
  client ignores the port in the endpoint and always dials `443`, so bind the
  endpoint there for Java (`AdminTlsPort=443` or `WithAdminTls(port: 443)`,
  portless connection string); see `certs/java.md`. Contributed by
  @alt-Rational (#117).

### Changed
- Dependency updates (no functional change): `Microsoft.NET.Test.Sdk` 18.10.1
  (#122), `Aspire.Hosting` 13.5.4 (#123), `System.Security.Cryptography.Pkcs`
  10.0.12 (#128), `vue` 3.5.43 and `@vitejs/plugin-vue` 6.0.9 (dashboard)
  (#126, #120), `java-jdk` 21.0.12+101.0.LTS (#121) and the
  Maven `exec-maven-plugin` 3.6.4 (#118) for the client-SDK smoke tests, and
  the `external/NServiceBus.Transport.AzureServiceBus` submodule to `2b73ed3`
  (#119).

### Fixed
- **Atom-XML management responses the Node.js and Python SDKs reject** (found
  while making their admin clients work over TLS, #117): entity `<id>` and
  `link href` now carry the full `{topic}/Subscriptions/{subscription}` (and
  `.../Rules/{rule}`) path so the SDKs can parse entity identities;
  subscription descriptions now emit `DeadLetteringOnFilterEvaluationExceptions`,
  `Status`, `AutoDeleteOnIdle` and `EntityAvailabilityStatus`, which those SDKs
  throw on when absent; and listing rules no longer 404s on the trailing slash
  the SDK appends.

## [0.6.0] - 2026-09-11

### Added
- **Docker image.** `docker/build-docker.sh` publishes the host (dashboard
  included) and packages it into a minimal runtime image; `docker/README.md`
  covers `docker run` and a Compose example. Inside a container the emulator
  binds `0.0.0.0`, advertises its container hostname in the printed connection
  string, and treats that hostname (or `ASB_HOST`, which also accepts
  comma-separated aliases) as the `default` namespace, so
  `RootManageSharedAccessKey` keeps working when other containers reach it by
  service name. `ASB_BIND_HOST` overrides the bind interface. Contributed by
  @alt-Rational (#108).
- **Subscription dead-letter queues in the dashboard.** Subscriptions have
  their own dead-letter queue in the broker, but the dashboard could not reach
  it: subscription rows carried no dead-letter count and had no click target,
  and topics whose only messages were dead-lettered were hidden from the
  sidebar. Subscriptions now open their own Messages and Dead Letter views,
  backed by subscription-scoped API routes, and their queues are attached to
  the live event stream so the views and sidebar badges update without a
  refresh. Contributed by @johnkors (#112).
- **Dashboard icons.** The sidebar, entity header, tabs and message rows use
  [lucide](https://lucide.dev) icons (`lucide-vue-next`) for queues, topics,
  subscriptions, dead-letter queues and message state instead of hand-picked
  text glyphs, and the count badges have tooltips. One `EntityIcon` component
  decides which icon stands for which kind of entity. (#114)

### Changed
- **Dashboard API routes are one route per operation.** The dashboard's JSON
  API used catch-all routes (`/queues/{**path}`) that inspected the tail of the
  path to decide between `/messages`, `/deadletter` and `/properties`. Each
  operation is now its own route, with subscriptions addressed as
  `/topics/{topic}/subscriptions/{subscription}/...`. Entity names, which may
  contain slashes, travel percent-encoded as a single path segment
  (`orders%2Feu` rather than `orders/eu`). The dashboard was updated to match;
  scripts that call the API directly need the same encoding.

### Fixed
- **Node.js connections hung for ~60 s when several were opened at once.**
  AMQPNetLite's listener pipelines its AMQP header and `open` straight after
  the `sasl-outcome`; rhea (the Node transport) stops parsing at the
  `sasl-outcome` and parks the rest of that TCP chunk until the next socket
  data event, which never comes. The emulator's proxy now withholds the
  server's AMQP header until the client's header has been forwarded, so it
  always lands in a later chunk. Diagnosed, with a regression test, by
  @alt-Rational (#109); moved from a patched AMQPNetLite into the proxy in
  #113, and the equivalent listener-side change is proposed upstream in
  Azure/amqpnetlite#651.
- **Batch sends from the Node.js SDK lost all but a garbled first message.** A
  batch is one AMQP transfer whose body is a list of encoded messages. The
  emulator recognised it only when the envelope had no `Subject`, which holds
  for the .NET SDK but not for `@azure/service-bus`, whose `sendMessages(array)`
  copies the first message's properties onto the envelope. Batches are now
  detected by the transfer's message-format (`0x80013700`), which every SDK
  sets. The Node.js, Python and Java smoke tests each gained a subject-bearing
  batch step. (#115)

## [0.5.0] - 2026-09-10

### Added
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
