# Message TTL / expiry — design

## Why

`TimeToLive`, `DefaultMessageTimeToLive` and `DeadLetteringOnMessageExpiration` are parsed,
stored and echoed everywhere, but nothing enforces them: expired messages are still delivered,
never swept and never dead-lettered. Found while triaging #193.

## Azure behaviour (measured on george-dev, 4 Nov 2026)

Throwaway probes against a real namespace gave these results. Where they differ from what the
request assumed, the measured behaviour wins.

| Question | Azure |
|---|---|
| Message TTL larger than entity default | **Received** message shows the entity default (header `ttl`, `AbsoluteExpiryTime` recomputed). |
| Message with no TTL | Received message shows the entity default. On an entity whose default is "max", there's no `ttl` header and `ExpiresAt` = `DateTimeOffset.MaxValue`. |
| Topic → subscription | Both caps apply: topic default 40s, subscription 1h → 40s; subscription 20s → 20s. |
| **Peek** | Shows the TTL **as sent** (1h, or none), not the capped one. Also returns messages that have already expired but haven't been removed yet. |
| Expired, never received | Stays in `ActiveMessageCount` for a while (still there after 8s). It's removed when a receiver pulls, or later by Azure's own lazy background pass (seen once after ~4s). |
| Dead-letter reason / description | `TTLExpiredException` / `The message expired and was dead lettered.` |
| `DeadLetteringOnMessageExpiration = false` | Message is dropped; the DLQ stays empty. |
| Locked past expiry | `Complete` succeeds. `Abandon` succeeds, and the message then expires (dead-lettered, delivery count 2 as seen by the SDK). |
| Sessions | Expired session message isn't delivered on accept; it's dead-lettered. |
| **Scheduled** | The clock starts at **send time**, not activation: a message with a 4s TTL scheduled 6s ahead goes straight to the DLQ when it activates. After activation, `EnqueuedTime` becomes the activation time. |
| `ForwardDeadLetteredMessagesTo` | Expired message is forwarded with reason/description. At the destination, `EnqueuedTime` is reset to the arrival time and the TTL clock **restarts** (it expired there 3s after arriving). |
| `ForwardTo` | Destination `EnqueuedTime` is the arrival time; the message expires there by its TTL. |
| Wire format | SDK sends `CreationTime` and `AbsoluteExpiryTime` (client clock) when TTL is set; Azure passes `CreationTime` through and recomputes `AbsoluteExpiryTime = CreationTime + effective TTL`. |

## Goals

1. Effective TTL at enqueue = min(message TTL, entity default), with topic then subscription caps.
   Server-side `ExpiresAtUtc = EnqueuedTimeUtc + effective TTL` (saturating).
2. Never deliver an expired message (PeekLock, ReceiveAndDelete, sessions). On dequeue, an expired
   message is expired in place (dead-lettered or dropped) and the next one is tried.
3. Background sweep (`MessageExpiryProcessor`, modelled on `ScheduledMessageProcessor`) walks every
   namespace's queues and subscriptions; per-entity and per-message failures are logged and skipped.
   Locked messages are left alone; they expire once abandoned, released or their lock lapses.
4. Expiry dead-letters through `QueueEntity.MoveToDeadLetterQueue`, the same exit as explicit and
   max-delivery dead-lettering, so #193's `ForwardDeadLetteredMessagesTo` applies. DLQs never expire.
5. Received messages report the effective TTL and `AbsoluteExpiryTime`; peek reports the TTL as sent.
6. Auto-forwarding (both kinds) resets `EnqueuedTimeUtc` to arrival time, so the clock restarts at
   the destination.

## Non-goals

- Deferred messages: Microsoft documents that they don't move to the DLQ on expiry; left as is.
- Updating `EnqueuedTime` to activation time for scheduled messages (an `EnqueuedTime` fidelity
  issue, not TTL). Expiry already starts from the send time, which is what Azure does.
- Passing through the client's `CreationTime`. `ExpiresAt` is server enqueue time + TTL, which
  differs from Azure only by client/server clock skew.
- Moving lock expiry onto `TimeProvider` (#135's wider ask).
- Keeping expired messages peekable until a receiver pulls. The emulator's sweep removes them
  within about a second, which falls within Azure's documented "eventually".
- Dropping `x-opt-dead-letter-source` for local DLQ messages (an existing difference, shared with
  explicit dead-lettering).

## Design

- **Clock**: `TimeProvider` (BCL). `NamespaceRegistry`/`NamespaceContext` take an optional one
  (default `TimeProvider.System`), and pass it to queues via `SetNamespace`. `QueueEntity.TimeProvider`
  is settable for unit tests. Tests use `Microsoft.Extensions.TimeProvider.Testing`'s `FakeTimeProvider`.
- **BrokeredMessage**: `EffectiveTimeToLive` (defaults to `TimeToLive` until an entity caps it),
  `ExpiresAtUtc` (default `MaxValue`), and an availability claim (`MarkAvailable` / `TryClaim`,
  interlocked) so a sweep and a dequeuer can't both take a message that's still sitting in a channel.
- **QueueEntity**: `Enqueue` stamps the effective TTL and expiry (except on DLQs). The channel and
  session dequeue paths skip stale or expired entries. `ExpireMessages()` runs the sweep for one entity;
  for session queues it also takes the message out of the session's priority queue, so next-available
  doesn't pick a session that holds only expired messages.
- **TopicEntity.Publish**: caps the clone's effective TTL by the topic default; the subscription's
  queue then caps it by its own default (`SubscriptionEntity.DefaultMessageTimeToLive` now forwards to
  the queue).
- **NamespaceContext.TryTransfer**: resets `EnqueuedTimeUtc` before enqueueing at the destination.
- **Host / TestHost**: start `MessageExpiryProcessor` next to the scheduled processor (1s interval).

## Tests

- Unit (`AlmostServiceBus.Tests`, `FakeTimeProvider`): caps, topic/subscription caps, dequeue skips
  expired (DLQ vs drop), sessions, locked messages, sweep resilience, DLQ never expires, scheduled
  clock, forwarding to `ForwardDeadLetteredMessagesTo`.
- SDK end-to-end (`SdkIntegration.Tests`): received TTL/ExpiresAt, expiry → DLQ with reason, drop.
- Conformance (`Conformance.Tests`, emulator + real Azure): received TTL capped, peek shows the TTL as
  sent, expired → DLQ with reason/description, dropped without DLQ, session, scheduled clock.
  The Azure connection string comes from env var `ASB_CONNECTION_STRING`, falling back to user secret
  `almostservicebus-conformance`.
