# Message TTL / Expiry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enforce message TTL in the broker: cap TTL at enqueue, never deliver expired messages, sweep them, and dead-letter (`TTLExpiredException`) or drop them.

**Architecture:** `QueueEntity` stamps an effective TTL and a server-side expiry at enqueue. Its dequeue paths expire stale messages in place, and a `MessageExpiryProcessor` sweeps every namespace periodically. Expiry dead-letters through the existing `MoveToDeadLetterQueue`, so #193's forwarding applies. The clock is a `TimeProvider`.

**Tech Stack:** .NET 10, AMQPNetLite, xUnit, `Microsoft.Extensions.TimeProvider.Testing`, Azure.Messaging.ServiceBus 7.21.

**Spec:** `docs/superpowers/specs/2026-11-04-message-ttl-expiry-design.md`

## Global Constraints

- Dead-letter reason `TTLExpiredException`, description `The message expired and was dead lettered.`
- DLQs never expire messages; locked (pending) and deferred messages are never expired by the sweep.
- Peek shows the TTL as sent; receive shows the effective TTL.
- Conformance tests: unique entity prefix, TTLs of a few seconds, never print the connection string.

## Review Focus

- Message in a redelivery delay (abandoned, 1s wait) when it expires: must not be delivered once it becomes available. Test in Task 2.
- Session that holds only expired messages: `AcceptNextSession` must not pick it after a sweep. Test in Task 3.
- `TimeToLive = TimeSpan.MaxValue` plus `EnqueuedTimeUtc`: the expiry sum must saturate, not overflow. Test in Task 1.
- Sweep racing a dequeuer: a message must be taken by exactly one of them (the claim flag). Covered by design; stress test in Task 2.
- Purge on the dashboard (`TryDequeueImmediate` loop) must still terminate when stale entries remain in the channel. Test in Task 2.

---

### Task 1: Clock + effective TTL at enqueue

**Files:** `Broker/BrokeredMessage.cs`, `Broker/QueueEntity.cs`, `Broker/TopicEntity.cs`, `Broker/SubscriptionEntity.cs`, `Broker/NamespaceContext.cs`, `Broker/NamespaceRegistry.cs`, `Amqp/ReceiverLinkEndpoint.cs`, `Amqp/ManagementLinkEndpoint.cs`; test `tests/AlmostServiceBus.Tests/Broker/MessageExpiryTests.cs`

**Produces:** `BrokeredMessage.EffectiveTimeToLive : TimeSpan` (get falls back to `TimeToLive`), `BrokeredMessage.ExpiresAtUtc : DateTimeOffset` (default `MaxValue`), `BrokeredMessage.IsExpired(DateTimeOffset now) : bool`; `QueueEntity.TimeProvider : TimeProvider`; `NamespaceContext(string, MessageEventBus?, TimeProvider?)` with `TimeProvider` property; `NamespaceRegistry(MessageEventBus?, TimeProvider?)`; `ReceiverLinkEndpoint.ConvertToAmqpMessage(BrokeredMessage, bool peek = false)`.

- [ ] Tests: `Enqueue_CapsTtlAtEntityDefault` (TTL 1h, default 30s → effective 30s, ExpiresAt = enq+30s), `Enqueue_NoTtl_UsesEntityDefault`, `Enqueue_MaxTtlAndMaxDefault_ExpiresAtMaxValue`, `Publish_AppliesTopicThenSubscriptionCap` (topic 40s; subs 20s/1h → 20s/40s), `DeadLetterQueue_DoesNotStampExpiry`, `ConvertToAmqpMessage_ReceiveUsesEffectiveTtl_PeekUsesSentTtl`.
- [ ] Run, confirm they fail.
- [ ] Implement. Saturating add: if `ttl >= DateTimeOffset.MaxValue - enq` → `MaxValue`. `SubscriptionEntity.DefaultMessageTimeToLive` forwards to `Queue`. `TopicEntity.Publish` sets `clone.EffectiveTimeToLive = Min(clone.EffectiveTimeToLive, DefaultMessageTimeToLive)`.
- [ ] Run, confirm they pass, then commit.

### Task 2: Expire on dequeue + sweep (non-session)

**Files:** `Broker/BrokeredMessage.cs`, `Broker/QueueEntity.cs`; test `MessageExpiryTests.cs`

**Produces:** `BrokeredMessage.MarkAvailable()`, `BrokeredMessage.TryClaim() : bool`; `QueueEntity.ExpireMessages() : int` (number expired).

- [ ] Tests: `TryDequeueImmediate_SkipsExpired_DeadLettersWhenEnabled` (reason/description exact), `TryDequeueImmediate_SkipsExpired_DropsWhenDisabled` (DLQ empty, MessageCount 0), `DequeueAsync_SkipsExpired`, `ExpireMessages_SweepsWithoutReceiver`, `ExpireMessages_LeavesLockedMessage_ThenExpiresAfterAbandon`, `Complete_AfterTtl_WhileLocked_Succeeds`, `DeadLetterQueue_NeverExpires`, `TryDequeueImmediate_ReturnsNullWhenOnlyStaleEntries`, `Expired_ForwardsToForwardDeadLetteredMessagesTo`, `ConcurrentSweepAndDequeue_EachMessageTakenOnce`.
- [ ] Run, confirm they fail.
- [ ] Implement: `MarkAvailable` before every channel/session write; dequeue loops `TryClaim`, then expiry check → `Expire(message)`. `Expire` calls `MoveToDeadLetterQueue(lockToken, message, "TTLExpiredException", "The message expired and was dead lettered.")` or drops (`_allMessages` remove). Both decrement `_messageCount`.
- [ ] Run, confirm they pass, then commit.

### Task 3: Sessions

**Files:** `Broker/SessionManager.cs` (`SessionState.TryRemove(BrokeredMessage) : bool`), `Broker/QueueEntity.cs`; test `MessageExpiryTests.cs`

- [ ] Tests: `TryDequeueFromSession_SkipsExpired`, `ExpireMessages_RemovesFromSession_SoNextAvailableSkipsIt`.
- [ ] Run, confirm they fail. Implement (`PriorityQueue.Remove` under the session lock). Run, confirm they pass, then commit.

### Task 4: Forwarding clock + scheduled + background processor

**Files:** `Broker/NamespaceContext.cs` (`TryTransfer` resets `EnqueuedTimeUtc` from the TimeProvider), create `Broker/MessageExpiryProcessor.cs`, `Host/Program.cs`, `TestHost/ServiceBusEmulatorFixture.cs`; test `MessageExpiryTests.cs`

**Produces:** `MessageExpiryProcessor(NamespaceRegistry)`, `void ProcessExpiredMessages()`, `void StartBackground(TimeSpan)`, `IDisposable`.

- [ ] Tests: `ForwardTo_RestartsClockAtDestination`, `ScheduledMessage_ClockStartsAtSend` (expired on activation → DLQ), `Processor_SweepsAllNamespacesQueuesAndSubscriptions`, `Processor_ContinuesAfterOneEntityThrows`.
- [ ] Run, confirm they fail. Implement, wire up at a 1s interval. Run, confirm they pass, then commit.

### Task 5: SDK end-to-end + conformance + CHANGELOG

**Files:** `tests/AlmostServiceBus.SdkIntegration.Tests/MessageExpiryTests.cs`, `tests/AlmostServiceBus.Conformance.Tests/ConformanceTestBase.cs` (+ TTL tests), `RealAsbConformanceTests.cs` (user-secret fallback), the conformance csproj (`UserSecretsId`), `CHANGELOG.md`.

- [ ] SDK tests: received TTL capped / ExpiresAt; expiry → DLQ reason+description; drop; session.
- [ ] Conformance tests (both targets): `Ttl_ReceivedMessageIsCappedAtEntityDefault`, `Ttl_PeekShowsSentTtl`, `Ttl_ExpiredMessageDeadLettered`, `Ttl_ExpiredMessageDroppedWithoutDeadLettering`, `Ttl_ExpiredSessionMessageDeadLettered`, `Ttl_ScheduledMessageClockStartsAtSend`.
- [ ] Run against the emulator and real Azure; run the full suite; update the CHANGELOG; commit.
