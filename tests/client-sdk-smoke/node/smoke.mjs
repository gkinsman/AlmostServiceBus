// Smoke test: the official Node.js Azure Service Bus SDK (@azure/service-bus, rhea transport)
// against a running AlmostServiceBus emulator. Same scenario as the Python and Java smoke
// tests (see ../README.md). Exits non-zero on the first failed check.
//
// Entities are created through the emulator's plain-HTTP Atom management API with fetch(). The
// SDK's ServiceBusAdministrationClient cannot be used: it always speaks HTTPS to the endpoint
// host and the emulator, like Microsoft's, is plaintext-only. The data plane does honour
// UseDevelopmentEmulator=true (via @azure/core-amqp) for loopback endpoints.
//
//   ASB_CONNECTION_STRING=... ASB_ADMIN_ENDPOINT=http://localhost:5300 node smoke.mjs

import { ServiceBusClient } from "@azure/service-bus";
import { randomUUID } from "node:crypto";

const CONNECTION_STRING =
  process.env.ASB_CONNECTION_STRING ??
  "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
const ADMIN_ENDPOINT = (process.env.ASB_ADMIN_ENDPOINT ?? "http://localhost:5300").replace(/\/+$/, "");
const RUN = randomUUID().replace(/-/g, "").slice(0, 8);
const QUEUE = `smoke-node-${RUN}`;
const SESSION_QUEUE = `smoke-node-sessions-${RUN}`;
const TOPIC = `smoke-node-topic-${RUN}`;
const SUBSCRIPTION = "worker";
const WAIT_MS = 10_000;
const SB_NS = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";

let stepNo = 0;
const step = (name) => console.log(`[${String(++stepNo).padStart(2, "0")}] ${name}`);
function check(cond, what) {
  if (!cond) {
    // Throw rather than process.exit(): exiting from inside an async chain can drop buffered
    // stdout, which hides which step failed in CI logs.
    throw new Error(`check failed: ${what}`);
  }
  console.log(`    ok   ${what}`);
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ── plain-HTTP management helpers ────────────────────────────────────────────

function keyName() {
  const m = /SharedAccessKeyName=([^;]+)/.exec(CONNECTION_STRING);
  return m ? m[1] : "RootManageSharedAccessKey";
}

async function mgmt(method, path, body) {
  const res = await fetch(`${ADMIN_ENDPOINT}/${path}?api-version=2021-05`, {
    method,
    headers: {
      // The emulator maps SharedAccessKeyName to a namespace; the signature is not checked.
      Authorization: `SharedAccessSignature sr=localhost&sig=x&se=1&skn=${keyName()}`,
      "Content-Type": "application/atom+xml;type=entry;charset=utf-8",
    },
    body,
  });
  return { status: res.status, text: await res.text() };
}

const entry = (tag, inner) =>
  `<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml">` +
  `<${tag} xmlns="${SB_NS}" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">${inner}</${tag}></content></entry>`;
const created = (r) => r.status === 200 || r.status === 201;

// ── SDK helpers ──────────────────────────────────────────────────────────────

async function receiveOne(receiver, waitMs = WAIT_MS) {
  const msgs = await receiver.receiveMessages(1, { maxWaitTimeInMs: waitMs });
  check(msgs.length === 1, `received one message (got ${msgs.length})`);
  return msgs[0];
}

async function main() {
  step("management (plain HTTP Atom API): create queue, session queue, topic, subscription");
  let r = await mgmt("PUT", QUEUE, entry("QueueDescription", "<LockDuration>PT1M</LockDuration><MaxDeliveryCount>3</MaxDeliveryCount>"));
  check(created(r), `create queue -> ${r.status}`);
  r = await mgmt("GET", QUEUE);
  check(r.status === 200 && r.text.includes("<LockDuration>PT1M</LockDuration>"), "get queue echoes LockDuration");
  check(r.text.includes("<MaxDeliveryCount>3</MaxDeliveryCount>"), "get queue echoes MaxDeliveryCount");
  r = await mgmt("PUT", SESSION_QUEUE, entry("QueueDescription", "<RequiresSession>true</RequiresSession>"));
  check(created(r), `create session queue -> ${r.status}`);
  r = await mgmt("PUT", TOPIC, entry("TopicDescription", ""));
  check(created(r), `create topic -> ${r.status}`);
  r = await mgmt("PUT", `${TOPIC}/subscriptions/${SUBSCRIPTION}`, entry("SubscriptionDescription", ""));
  check(created(r), `create subscription -> ${r.status}`);
  r = await mgmt("GET", "$Resources/queues");
  check(r.status === 200 && r.text.includes(QUEUE), "list queues includes the queue");

  const client = new ServiceBusClient(CONNECTION_STRING);
  try {
    step("send with application properties, receive (PeekLock), complete");
    const sender = client.createSender(QUEUE);
    await sender.sendMessages({
      body: { orderId: "1" },
      messageId: "msg-1",
      subject: "OrderPlaced",
      correlationId: "corr-1",
      contentType: "application/json",
      applicationProperties: { source: "smoke-node", attempt: 1 },
    });
    const receiver = client.createReceiver(QUEUE, { receiveMode: "peekLock" });
    let m = await receiveOne(receiver);
    check(m.body?.orderId === "1", "body round-trips");
    check(m.messageId === "msg-1", `messageId round-trips (${m.messageId})`);
    check(m.subject === "OrderPlaced", `subject round-trips (${m.subject})`);
    check(m.correlationId === "corr-1", `correlationId round-trips (${m.correlationId})`);
    check(m.contentType === "application/json", `contentType round-trips (${m.contentType})`);
    check(m.applicationProperties?.source === "smoke-node", `string application property round-trips (${m.applicationProperties?.source})`);
    check(m.applicationProperties?.attempt === 1, `int application property round-trips (${m.applicationProperties?.attempt})`);
    const firstDeliveryCount = m.deliveryCount;
    check(typeof m.sequenceNumber !== "undefined", `sequenceNumber assigned (${m.sequenceNumber})`);
    check(m.lockedUntilUtc instanceof Date, "lockedUntilUtc set");

    step("renew lock");
    const before = m.lockedUntilUtc;
    await sleep(1100);
    const after = await receiver.renewMessageLock(m);
    check(after > before, `lockedUntil moved forward (${before.toISOString()} -> ${after.toISOString()})`);
    await receiver.completeMessage(m);

    step("peek");
    await sender.sendMessages([{ body: "p1" }, { body: "p2" }]);
    const peeked = await receiver.peekMessages(5);
    check(peeked.length === 2, `peek sees both messages (${peeked.length})`);
    check(peeked[0].sequenceNumber < peeked[1].sequenceNumber, "peek is in sequence order");
    for (let i = 0; i < 2; i++) await receiver.completeMessage(await receiveOne(receiver));
    check((await receiver.peekMessages(5)).length === 0, "queue empty after completing both");

    step("batch send where the first message has a subject");
    // sendMessages(array) builds one AMQP batch transfer. The Node SDK copies the first
    // message's properties (subject included) onto the batch envelope, which used to make the
    // emulator treat the whole batch as a single message.
    await sender.sendMessages([
      { body: "b1", subject: "OrderPlaced", applicationProperties: { n: 1 } },
      { body: "b2", subject: "OrderPlaced", applicationProperties: { n: 2 } },
      { body: "b3", subject: "OrderShipped", applicationProperties: { n: 3 } },
    ]);
    const batch = await receiver.receiveMessages(5, { maxWaitTimeInMs: WAIT_MS });
    check(batch.length === 3, `all three batched messages arrive as separate messages (${batch.length})`);
    check(batch.map((m) => m.body).join() === "b1,b2,b3", `bodies intact (${batch.map((m) => m.body)})`);
    check(batch[2].subject === "OrderShipped" && batch[2].applicationProperties?.n === 3, "each message keeps its own subject and properties");
    for (const m of batch) await receiver.completeMessage(m);

    step("abandon -> redelivery bumps deliveryCount");
    await sender.sendMessages({ body: "retry-me" });
    const first = await receiveOne(receiver);
    await receiver.abandonMessage(first);
    const second = await receiveOne(receiver);
    check(second.body === "retry-me", "abandoned message is redelivered");
    check(second.deliveryCount === firstDeliveryCount + 1, `deliveryCount is one higher after abandon (${firstDeliveryCount} -> ${second.deliveryCount})`);
    await receiver.completeMessage(second);

    step("dead-letter with reason, then receive from the dead-letter sub-queue");
    await sender.sendMessages({ body: "poison", messageId: "poison-1" });
    const bad = await receiveOne(receiver);
    await receiver.deadLetterMessage(bad, { deadLetterReason: "ValidationFailed", deadLetterErrorDescription: "schema mismatch" });
    const dlq = client.createReceiver(QUEUE, { subQueueType: "deadLetter" });
    const dead = await receiveOne(dlq);
    check(dead.messageId === "poison-1", "dead-lettered message is in the DLQ");
    check(dead.deadLetterReason === "ValidationFailed", `deadLetterReason (${dead.deadLetterReason})`);
    check(dead.deadLetterErrorDescription === "schema mismatch", `deadLetterErrorDescription (${dead.deadLetterErrorDescription})`);
    await dlq.completeMessage(dead);
    await dlq.close();

    step("scheduled message is not delivered early");
    const scheduledAt = Date.now();
    const seqs = await sender.scheduleMessages({ body: "later" }, new Date(scheduledAt + 3000));
    check(seqs.length === 1, `scheduleMessages returns a sequence number (${seqs})`);
    check((await receiver.receiveMessages(1, { maxWaitTimeInMs: 1000 })).length === 0, "nothing arrives in the first second");
    const late = await receiveOne(receiver);
    const elapsed = (Date.now() - scheduledAt) / 1000;
    check(late.body === "later", "scheduled message arrives");
    check(elapsed >= 2.5, `...and not before the scheduled time (${elapsed.toFixed(1)}s after scheduling)`);
    await receiver.completeMessage(late);
    await receiver.close();
    await sender.close();

    step("sessions: FIFO within a session, session state, next-available session");
    const sessionSender = client.createSender(SESSION_QUEUE);
    await sessionSender.sendMessages([0, 1, 2].map((i) => ({ body: `s1-${i}`, sessionId: "s1" })));
    await sessionSender.sendMessages({ body: "s2-0", sessionId: "s2" });
    await sessionSender.close();
    const s1 = await client.acceptSession(SESSION_QUEUE, "s1");
    check(s1.sessionId === "s1", "accepted the requested session");
    const held = await s1.receiveMessages(3, { maxWaitTimeInMs: WAIT_MS });
    const got = held.map((x) => x.body);
    check(JSON.stringify(got) === JSON.stringify(["s1-0", "s1-1", "s1-2"]), `session messages arrive in order (${got})`);
    await s1.setSessionState("checkpoint-3");
    check((await s1.getSessionState()) === "checkpoint-3", "session state round-trips");
    await s1.renewSessionLock();
    for (const x of held) await s1.completeMessage(x);
    await s1.close();
    const next = await client.acceptNextSession(SESSION_QUEUE);
    check(next.sessionId === "s2", `next-available session is s2 (${next.sessionId})`);
    const s2 = await receiveOne(next);
    check(s2.body === "s2-0", "s2 message received");
    await next.completeMessage(s2);
    await next.close();

    step("topic -> subscription");
    const topicSender = client.createSender(TOPIC);
    await topicSender.sendMessages({ body: "via-topic", applicationProperties: { k: "v" } });
    const sub = client.createReceiver(TOPIC, SUBSCRIPTION);
    const t = await receiveOne(sub);
    check(t.body === "via-topic", "subscription receives the published message");
    check(t.applicationProperties?.k === "v", "application property survives topic fan-out");
    await sub.completeMessage(t);
    await sub.close();
    await topicSender.close();
  } finally {
    await client.close();
  }

  step("management (plain HTTP Atom API): delete entities");
  for (const path of [QUEUE, SESSION_QUEUE, TOPIC]) {
    r = await mgmt("DELETE", path);
    check(r.status === 200 || r.status === 204, `delete ${path} -> ${r.status}`);
  }
  r = await mgmt("GET", QUEUE);
  check(r.status === 404, "deleted queue is gone (404)");

  console.log("\nNode SDK smoke test passed");
}

main().catch((err) => {
  console.error("    FAIL:", err instanceof Error ? err.message : err);
  process.exitCode = 1;
});
