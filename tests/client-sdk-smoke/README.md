# Client SDK smoke tests

End-to-end checks that the official **Python**, **Node.js** and **Java** Azure Service Bus SDKs
work against a running AlmostServiceBus emulator. They run in CI (`Client SDK: …` jobs) and are
what backs the README's compatibility table for those SDKs. The .NET SDK is covered by the
internal test suites and by MassTransit / Wolverine / NServiceBus running their own suites.

Each script drives the same scenario and stops at the first failed check:

| Step | What is exercised |
|------|-------------------|
| Management | create queue (lock duration, max delivery count), session queue, topic + subscription; get / list / delete |
| Send / receive | body, message id, subject, correlation id, content type, application properties (string and int), sequence number, lock |
| Lock renewal | `locked-until` moves forward |
| Peek | two messages, in sequence order; empty after completion |
| Abandon | redelivery with delivery count + 1 |
| Dead-letter | with reason and description; received from the `$DeadLetterQueue` sub-queue with both intact |
| Sessions | FIFO within a session, session state round-trip, session lock renewal, next-available session |
| Topic | publish → subscription receive, application properties preserved |
| Scheduled | scheduled message is not delivered early and arrives after the scheduled time |

Each language also has an **admin test** (`admin.py` / `admin.mjs` / `AdminSmoke`) that exercises
the management interface end-to-end — queue / topic / subscription / rule create, get, list,
update and delete, plus a data-plane send/receive on an admin-created entity. Node and Python use
the SDK's `ServiceBusAdministrationClient` against the HTTPS admin endpoint (`5301`); Java drives
the same Atom REST API over HTTPS directly (its SDK admin client can't reach the port — see
below). See [../../certs/README.md](../../certs/README.md) for per-language CA trust setup.

## Running locally

Start the emulator on its default ports (AMQP `5672`, admin HTTP `5300`):

```bash
dotnet run --project src/AlmostServiceBus.Host
```

Then, in another shell:

```bash
# Python (3.10+)
cd tests/client-sdk-smoke/python
pip install -r requirements.txt
python smoke.py

# Node.js (20+)
cd tests/client-sdk-smoke/node
npm ci
node smoke.mjs
node concurrent-connections.mjs   # concurrency regression guard, see below

# Java (17+, Maven)
cd tests/client-sdk-smoke/java
mvn -q compile exec:java
```

`ASB_CONNECTION_STRING` overrides the connection string (default is the emulator's
`RootManageSharedAccessKey` string with `UseDevelopmentEmulator=true`); `ASB_ADMIN_ENDPOINT`
overrides the admin API base URL (default `http://localhost:5300`).

### Admin tests (HTTPS admin endpoint)

Start the emulator with the HTTPS admin endpoint enabled and a cert directory:

```bash
dotnet run --project src/AlmostServiceBus.Host -- --AdminTlsPort 5301 --AdminTlsCertDir /tmp/asb-certs
```

The emulator writes a CA (`emulator-ca.crt`) into that directory on first run. Each client trusts
it differently (see [../../certs/README.md](../../certs/README.md)):

```bash
# Python — the requests transport honours REQUESTS_CA_BUNDLE
cd tests/client-sdk-smoke/python
REQUESTS_CA_BUNDLE=/tmp/asb-certs/emulator-ca.crt python admin.py

# Node.js — node ignores the OS store; use NODE_EXTRA_CA_CERTS
cd tests/client-sdk-smoke/node
NODE_EXTRA_CA_CERTS=/tmp/asb-certs/emulator-ca.crt node admin.mjs

# Java — trust the CA via a PKCS12 truststore, run the AdminSmoke main
cd tests/client-sdk-smoke/java
keytool -importcert -noprompt -alias asb-ca -file /tmp/asb-certs/emulator-ca.crt \
  -keystore /tmp/asb-certs/emulator-truststore.p12 -storetype PKCS12 -storepass changeit
MAVEN_OPTS="-Djavax.net.ssl.trustStore=/tmp/asb-certs/emulator-truststore.p12 -Djavax.net.ssl.trustStoreType=PKCS12 -Djavax.net.ssl.trustStorePassword=changeit" \
  mvn -q -o exec:java -Dexec.mainClass=io.almostservicebus.smoke.AdminSmoke
```

`ASB_ADMIN_CONNECTION_STRING` (Node/Python, default `Endpoint=sb://localhost:5301;…`) and
`ASB_ADMIN_TLS_ENDPOINT` (Java, default `https://localhost:5301`) override the admin target.

## Node.js concurrent-connection regression test

`node/concurrent-connections.mjs` (contributed in #109) opens **6 clients** (6 AMQP connections)
at once, each with **4 senders + 4 receivers** sending **10 messages** per sender (240 total), and
asserts they all complete within a 30 s deadline. Knobs: `ASB_CONC_CLIENTS`, `ASB_CONC_LINKS`,
`ASB_CONC_MESSAGES`, `ASB_CONC_DEADLINE_MS`.

It guards a deadlock in the connection handshake. AMQPNetLite's listener pipelines its AMQP
protocol header and `open` immediately after the `sasl-outcome`, before it has seen the client's
header — legal AMQP, but rhea (Node's transport) stops parsing at the `sasl-outcome` frame and
parks the rest of that TCP chunk until the *next* socket data event. When the three arrive in one
segment, which happens readily under concurrent connects, that event never comes: the server has
sent everything and waits for `begin`, the client waits for a header it is already holding, and
the connection sits until rhea's ~60 s idle timeout. The emulator's `TcpMultiplexer` now withholds
the server's AMQP header until the client's header has been forwarded, so the header always
arrives in a later chunk — the same thing a server that waits for the client's header (as Azure
does) would produce. Without that, only 2 of the 6 connections typically come up before the
deadline; with it the test finishes in about a second.

## Management: plain HTTP (5300) vs. the SDK admin clients over TLS (5301)

The emulator's data plane is plaintext, exactly like Microsoft's own Service Bus emulator, so the
**smoke** tests create and delete entities through the emulator's Atom XML REST API on plain HTTP
(port `5300`) with each language's standard HTTP client, and spend their effort on the SDK **data
plane** (which honours `UseDevelopmentEmulator=true` in all three SDKs).

The emulator also exposes an optional **HTTPS admin endpoint** (port `5301`, `--AdminTlsPort`),
which the **admin** tests use to drive the SDK admin clients themselves:

- **Node.js** and **Python** `ServiceBusAdministrationClient` speak HTTPS to the endpoint host and
  port, so — pointed at `Endpoint=sb://localhost:5301;…` and told to trust the emulator CA — they
  work end-to-end. `admin.mjs` / `admin.py` cover queue/topic/subscription/rule CRUD plus usage.
- **Java** `ServiceBusAdministrationClientBuilder` strips the port and always dials the namespace
  host on 443, ignoring both the connection-string port and an explicit `.endpoint()` override, so
  it *cannot* reach the emulator. `AdminSmoke` therefore drives the same Atom REST API the admin
  client would, over HTTPS on 5301, with `java.net.http`.

This partially supersedes an earlier note (and CLAUDE.md decision #10) that no non-.NET admin
client could reach the emulator: Node and Python now can, over the TLS endpoint; only Java still
cannot. If you need management from Java against the emulator, call the REST API directly (over
HTTP on 5300 or HTTPS on 5301) as `AdminSmoke` does.

## Things these tests taught us about the emulator

Each of these was found by one of the SDKs and fixed in the emulator; the tests guard them:

- **Disposition replies must echo the client's outcome type.** AMQPNetLite's `Complete()` always
  replied `Accepted`; the Java SDK fails an abandon/defer/dead-letter whose reply type differs
  from its request. The dead-letter reply must be a *bare* `Rejected` (no error), because the
  .NET SDK treats a Rejected-with-error reply as the broker refusing the settlement.
- **AMQP maps may carry string keys.** rhea (Node) sends the dead-letter reason/description map
  with string keys; AMQPNetLite's `Fields` indexer throws on anything but `Symbol`, which
  aborted the settlement and made Node's `deadLetterMessage` time out.
- **A restated Flow must not zero credit.** pyamqp re-sends the identical Flow on every receive
  call; AMQPNetLite treats a zero-delta flow as "credit reduced" and drops the link credit to
  zero, so the receiver never got its message.
- **Sender links are registered by entity path.** pyamqp attaches to
  `amqps://host:port/queue`; scheduled messages resolved through the sender-link registry were
  routed to an entity literally named that.
- **Request-processor reply links are keyed per connection.** The Java SDK uses the same
  reply-to (`cbs-client-reply-to`) on every connection; keyed by reply-to alone, closing one
  connection removed the reply link another connection still needed and its next token refresh
  hung.

Differences that are *not* emulator bugs, and that the tests accommodate:

- pyamqp exposes the raw AMQP `delivery-count` header (0 on first delivery); the .NET SDK adds
  one. The tests assert the *increase* after an abandon rather than an absolute value.
- pyamqp returns application-property keys and session state as `bytes`.
- At shutdown pyamqp sends a `detach` after it has already `end`ed the session; the emulator
  (correctly) closes that connection with `amqp:not-found`, which shows up as a warning in the
  emulator log. Real Service Bus would do the same.
