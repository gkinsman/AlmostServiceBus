# AMQP Transaction Support — Design

**Date:** 2026-06-01
**Status:** Approved

## Goal

Make the emulator support AMQP transactions so that clients using
`Azure.Messaging.ServiceBus` with `System.Transactions.TransactionScope` —
including **cross-entity transactions**
(`ServiceBusClientOptions.EnableCrossEntityTransactions = true`) — can commit a
group of operations atomically.

The motivating consumer is a "drain-and-resend" replay pattern: receive a
message from one queue, send a copy to another entity, and complete the
original — all inside a single `TransactionScope` so that a crash mid-flight
leaves the broker in a consistent state (at most one message has its
`DeliveryCount` bumped, nothing is lost or duplicated).

Today the emulator **rejects** transaction coordinator links
(`EmulatorContainer.AttachLink`, returning `amqp:not-implemented`), so any
client that opens a `TransactionScope` fails immediately. This is currently
listed under "Known Gaps" in `CLAUDE.md`.

**Acceptance bar:** the real `Azure.Messaging.ServiceBus` SDK must drive a
`TransactionScope` end-to-end against the running emulator — commit applies all
effects, rollback applies none. This is verified by SDK integration tests, not
just unit tests.

## How AMQP transactions work (the protocol we must serve)

1. The client opens a **coordinator link** — an attach whose `Target` is an
   `Amqp.Transactions.Coordinator` (the client is the sender on this link, so
   the emulator's server-side link is a *receiver*).
2. The client sends a transfer whose body is a `Declare`. The coordinator
   allocates a transaction id (`txn-id`) and settles that delivery with a
   `Declared { TxnId }` outcome.
3. The client performs transactional work, tagging each delivery with a
   `TransactionalState { TxnId, … }` delivery-state:
   - **Transactional send** — a transfer on a normal sender link whose
     delivery-state is `TransactionalState { TxnId }`. The message must not
     become visible until the transaction commits. The receiver echoes a
     disposition of `TransactionalState { TxnId, Outcome = Accepted }`.
   - **Transactional settlement** — a disposition on a receiver link whose
     delivery-state is `TransactionalState { TxnId, Outcome }` (the inner
     `Outcome` is the real intent: Accepted = complete, Released = abandon,
     Rejected = dead-letter, Modified = defer/abandon). The settlement must not
     take effect until commit; the message stays locked meanwhile.
4. The client sends a transfer whose body is a `Discharge { TxnId, Fail }`:
   - `Fail = false` → **commit**: apply every buffered operation.
   - `Fail = true`  → **rollback**: discard every buffered operation.
   The coordinator settles the discharge delivery with `Accepted`.

### What AMQPNetLite 2.5.1 gives us

- `Amqp.Transactions.{Coordinator, Declare, Declared, Discharge, TransactionalState}`
  are public frame types.
- Both `MessageContext.DeliveryState` and `DispositionContext.DeliveryState`
  surface the incoming `TransactionalState`, so we can read the `txn-id` on
  transfers and dispositions.
- `ListenerLink.DisposeMessage(Message, DeliveryState, settled)` lets us send an
  arbitrary outcome (`Declared`, `TransactionalState`, `Accepted`, `Rejected`)
  back to the client.

What it does **not** give us: a server-side coordinator. `ResourceManager` is
internal and client-only. We implement the declare → commit/rollback state
machine ourselves.

## Architecture

```
Client                                   Emulator
  │  attach(Target=Coordinator)  ───────▶ EmulatorContainer: accept → TransactionCoordinatorEndpoint
  │  transfer(Declare)           ───────▶ TransactionManager.Declare() → txn-id
  │  ◀─ disposition(Declared{txn-id})
  │  transfer(msg, TxnState{txn})───────▶ SenderLinkEndpoint: BUFFER enqueue under txn
  │  ◀─ disposition(TxnState{txn,Accepted})
  │  disposition(complete,TxnState)─────▶ ReceiverLinkEndpoint: BUFFER settle under txn
  │  ◀─ disposition(TxnState{txn,Accepted})
  │  transfer(Discharge{txn,fail})──────▶ TransactionManager.Commit (apply all) | Rollback (discard)
  │  ◀─ disposition(Accepted)
```

One new state machine plus three integration touch-points.

## Components

### 1. `TransactionManager` + `Transaction` (new, broker-agnostic)

Location: `src/AlmostServiceBus.Core/Broker/Transactions/`.

- `Transaction` accumulates an **ordered list of commit `Action` delegates**
  (and an optional matching list of rollback `Action` delegates). The endpoints
  capture broker work as closures, so the manager itself never references AMQP
  or the broker — it only runs delegates.
- `TransactionManager`:
  - `byte[] Declare()` — allocate a globally-unique `txn-id` (16-byte GUID) and
    register a fresh `Transaction`. GUID bytes guarantee uniqueness across all
    connections and entities, so a single flat table is safe for cross-entity
    transactions.
  - `void Enlist(byte[] txnId, Action commit, Action? rollback = null)` — append
    a buffered operation. Throws `TransactionNotFoundException` for an unknown id.
  - `bool Commit(byte[] txnId)` — run all commit actions in order under a lock,
    then remove the transaction. Best-effort: a throwing action is logged and the
    rest still run (an in-memory broker can't truly two-phase-commit).
  - `bool Rollback(byte[] txnId)` — run rollback actions (if any), discard the
    transaction. Never throws.
  - Returns `false` from Commit/Rollback for an unknown id so the coordinator can
    reply `Rejected`.

Unit-testable in isolation with counter-incrementing closures.

### 2. `TransactionCoordinatorEndpoint` (new `LinkEndpoint`)

Location: `src/AlmostServiceBus.Core/Amqp/`.

Server-receiver for the coordinator link. `OnMessage` decodes the body:

- `Declare` → `mgr.Declare()` → `link.DisposeMessage(msg, new Declared { TxnId }, settled: true)`.
- `Discharge` → `mgr.Commit(txnId)` or `mgr.Rollback(txnId)` depending on
  `Fail` → `link.DisposeMessage(msg, new Accepted(), true)`, or `Rejected` with
  `amqp:transaction-unknown-id` when the manager reports an unknown id.

The body arrives as an `AmqpValue` wrapping the `Declare`/`Discharge` described
type; the endpoint handles both directly-typed and `AmqpValue`-wrapped forms.

### 3. `EmulatorContainer.AttachLink` — accept coordinator links

The existing `attach.Target is Coordinator` branch flips from rejection to
acceptance: build the `AttachContext` via the existing reflection helper and
`Complete(new TransactionCoordinatorEndpoint(mgr), credit)`. The shared
`TransactionManager` is injected into `EmulatorContainer` (alongside the existing
`SetNamespaceRegistry` wiring).

### 4. `SenderLinkEndpoint.OnMessage` — buffer transactional sends

If `messageContext.DeliveryState is TransactionalState ts`:
- Resolve the transaction by `ts.TxnId`.
- `mgr.Enlist(ts.TxnId, commit: () => RouteMessage(address, brokered))` — buffer
  the *whole* route so the sequence number and visibility happen at commit time,
  matching real Service Bus semantics.
- Echo `link.DisposeMessage(msg, new TransactionalState { TxnId = ts.TxnId, Outcome = new Accepted() }, true)`.

The non-transactional path is unchanged. Both queues and topics are covered
because both flow through `RouteMessage`. Batch messages: each decoded inner
message is enlisted individually under the same txn.

### 5. `ReceiverLinkEndpoint.OnDisposition` + `SessionReceiverLinkEndpoint`

If `dispositionContext.DeliveryState is TransactionalState ts`:
- `mgr.Enlist(ts.TxnId, commit: () => SettleMessage(lockToken, ts.Outcome))` —
  `SettleMessage` already maps Accepted/Released/Rejected/Modified to
  Complete/Abandon/DeadLetter/Defer.
- Echo `link.DisposeMessage(message, new TransactionalState { TxnId = ts.TxnId, Outcome = ts.Outcome }, true)`.
- The message remains locked until commit. On rollback nothing is applied, the
  lock expires, and redelivery bumps `DeliveryCount` — the exact "crash bumps at
  most one message" guarantee the replay pattern relies on.

`SessionReceiverLinkEndpoint` gets the same treatment in its disposition path.

### Wiring

A single `TransactionManager` is created in `AmqpServer` and passed to both
`EmulatorContainer` (coordinator endpoints) and `ServiceBusLinkProcessor`
(sender/receiver endpoints), so every link in the process shares one txn table.

## Error handling

- Unknown / already-discharged `txn-id` on a transfer, disposition, or discharge
  → `Rejected` with condition `amqp:transaction-unknown-id`.
- Commit runs under a lock, best-effort: a settlement whose lock already expired
  logs a warning (the transactional disposition was already echoed; we can't
  un-send it). Real usage keeps transactions short, so locks stay fresh.
- Rollback never throws.

## Testing

TDD throughout.

- **Unit — `TransactionManagerTests`:** declare issues unique ids; commit runs
  buffered actions in order exactly once; rollback discards and runs rollback
  actions; commit/rollback of an unknown id returns false; enlist on unknown id
  throws.
- **SDK integration (the acceptance bar)** — real `Azure.Messaging.ServiceBus`
  against the running emulator, mirroring the existing SDK-integration test
  pattern:
  - Commit: receive + complete a message and send a new one inside one
    `TransactionScope`; after `scope.Complete()` both effects are visible (the
    original is gone, the new message is receivable).
  - Rollback: same work but the scope is *not* completed; afterwards neither
    effect happened (original still receivable, no new message).
  - Cross-entity: with `EnableCrossEntityTransactions = true`, complete on queue
    A and send to queue B in one transaction; commit applies both.
- Update `CLAUDE.md`: move "AMQP Transactions" out of "Known Gaps" and record
  what is supported.

## Out of scope (YAGNI)

- Distributed / durable transactions and recovery across process restart.
- Transaction timeouts independent of message lock expiry.
- The unrelated Wolverine tracking-pipeline timeout noted in `CLAUDE.md`.
