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

# Java (17+, Maven)
cd tests/client-sdk-smoke/java
mvn -q compile exec:java
```

`ASB_CONNECTION_STRING` overrides the connection string (default is the emulator's
`RootManageSharedAccessKey` string with `UseDevelopmentEmulator=true`); `ASB_ADMIN_ENDPOINT`
overrides the admin API base URL (default `http://localhost:5300`).

## Why management goes over plain HTTP, not the SDK admin clients

The emulator is plaintext-only, exactly like Microsoft's own Service Bus emulator. Of the four
official SDKs, only the .NET `ServiceBusAdministrationClient` honours `UseDevelopmentEmulator=true`
and speaks HTTP to port 5300. The others cannot reach a plaintext management endpoint:

- **Python** `ServiceBusAdministrationClient` hard-codes `https://` + namespace.
- **Node.js** `ServiceBusAdministrationClient` speaks HTTPS to the endpoint host and port.
- **Java** `ServiceBusAdministrationClientBuilder` speaks HTTPS to the namespace host on 443,
  ignoring the endpoint's port and the `.endpoint()` override.

So these tests create and delete entities through the emulator's Atom XML REST API (the same API
the SDK admin clients use) with each language's standard HTTP client, and spend their effort on
the SDK **data plane**, which does honour the emulator flag in all three SDKs. If you need
management operations from Python, Node or Java against the emulator, call the REST API on port
5300 the same way, or put a TLS-terminating proxy in front of it.

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
