// Concurrency stress test: the Node.js Azure Service Bus SDK (@azure/service-bus, rhea transport)
// opening many AMQP connections at once against a running AlmostServiceBus emulator.
//
// This is a regression guard for https://github.com/Azure/amqpnetlite/pull/651. Unpatched,
// AMQPNetLite's listener writes its AMQP Open frame as soon as the socket is accepted, before it
// has read the peer's protocol header. rhea frequently coalesces its protocol header and Open into
// a single TCP segment; when several connections are established concurrently the listener and the
// client both wait for the other's header, so every connection stalls until a ~60s idle timeout.
// The patch defers the listener Open until the peer header arrives, so concurrent connects succeed
// immediately.
//
// Scenario: 6 clients (= 6 AMQP connections), each with 4 senders + 4 receivers, 10 messages per
// sender. All connections are opened concurrently to provoke the race. A hard wall-clock deadline
// turns the unpatched hang into a fast, clear failure instead of a multi-minute stall.
//
//   ASB_CONNECTION_STRING=... ASB_ADMIN_ENDPOINT=http://localhost:5300 node concurrent-connections.mjs

import { ServiceBusClient } from "@azure/service-bus";
import { randomUUID } from "node:crypto";

const CONNECTION_STRING =
  process.env.ASB_CONNECTION_STRING ??
  "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
const ADMIN_ENDPOINT = (process.env.ASB_ADMIN_ENDPOINT ?? "http://localhost:5300").replace(/\/+$/, "");

const CLIENTS = Number(process.env.ASB_CONC_CLIENTS ?? 6);
const LINKS_PER_CLIENT = Number(process.env.ASB_CONC_LINKS ?? 4);
const MESSAGES_PER_SENDER = Number(process.env.ASB_CONC_MESSAGES ?? 10);
// Comfortably above the patched runtime (a few seconds) and well below the unpatched ~60s idle
// hang, so this fails fast against an unpatched emulator.
const DEADLINE_MS = Number(process.env.ASB_CONC_DEADLINE_MS ?? 30_000);
const RECEIVE_WAIT_MS = 15_000;

const RUN = randomUUID().replace(/-/g, "").slice(0, 8);
const SB_NS = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
const queueName = (c, l) => `conc-node-${RUN}-c${c}-q${l}`;

let stepNo = 0;
const step = (name) => console.log(`[${String(++stepNo).padStart(2, "0")}] ${name}`);
function check(cond, what) {
  if (!cond) throw new Error(`check failed: ${what}`);
  console.log(`    ok   ${what}`);
}

// ── plain-HTTP management helpers (see smoke.mjs) ─────────────────────────────

function keyName() {
  const m = /SharedAccessKeyName=([^;]+)/.exec(CONNECTION_STRING);
  return m ? m[1] : "RootManageSharedAccessKey";
}

async function mgmt(method, path, body) {
  const res = await fetch(`${ADMIN_ENDPOINT}/${path}?api-version=2021-05`, {
    method,
    headers: {
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

// Reject if the whole scenario has not finished by the deadline. Without this the unpatched hang
// would keep the process alive for minutes; instead we surface a clear, fast failure.
function withDeadline(promise, ms) {
  let timer;
  const deadline = new Promise((_, reject) => {
    timer = setTimeout(
      () => reject(new Error(`deadline of ${ms}ms exceeded — connections did not all open in time ` +
        `(this is the symptom fixed by amqpnetlite PR #651)`)),
      ms,
    );
  });
  return Promise.race([promise.finally(() => clearTimeout(timer)), deadline]);
}

async function receiveExactly(receiver, count, waitMs) {
  const collected = [];
  const start = Date.now();
  while (collected.length < count && Date.now() - start < waitMs) {
    const batch = await receiver.receiveMessages(count - collected.length, {
      maxWaitTimeInMs: Math.max(1000, waitMs - (Date.now() - start)),
    });
    if (batch.length === 0) break;
    for (const m of batch) {
      await receiver.completeMessage(m);
      collected.push(m);
    }
  }
  return collected;
}

// One client's whole workload: open its connection, then run all its links concurrently.
async function runClient(clientIndex) {
  const client = new ServiceBusClient(CONNECTION_STRING);
  try {
    const linkWork = [];
    for (let l = 0; l < LINKS_PER_CLIENT; l++) {
      const queue = queueName(clientIndex, l);
      linkWork.push((async () => {
        const sender = client.createSender(queue);
        const receiver = client.createReceiver(queue, { receiveMode: "peekLock" });
        try {
          const batch = [];
          for (let i = 0; i < MESSAGES_PER_SENDER; i++) {
            batch.push({ body: `c${clientIndex}-q${l}-m${i}`, messageId: `c${clientIndex}-q${l}-m${i}` });
          }
          await sender.sendMessages(batch);
          const got = await receiveExactly(receiver, MESSAGES_PER_SENDER, RECEIVE_WAIT_MS);
          check(
            got.length === MESSAGES_PER_SENDER,
            `client ${clientIndex} queue ${l}: received ${got.length}/${MESSAGES_PER_SENDER}`,
          );
          return got.length;
        } finally {
          await sender.close();
          await receiver.close();
        }
      })());
    }
    const counts = await Promise.all(linkWork);
    return counts.reduce((a, b) => a + b, 0);
  } finally {
    await client.close();
  }
}

async function main() {
  const total = CLIENTS * LINKS_PER_CLIENT * MESSAGES_PER_SENDER;
  step(`create ${CLIENTS * LINKS_PER_CLIENT} queues (${CLIENTS} clients x ${LINKS_PER_CLIENT} links)`);
  const creations = [];
  for (let c = 0; c < CLIENTS; c++) {
    for (let l = 0; l < LINKS_PER_CLIENT; l++) {
      creations.push(
        mgmt("PUT", queueName(c, l), entry("QueueDescription", "<LockDuration>PT1M</LockDuration>")).then((r) =>
          check(created(r), `create ${queueName(c, l)} -> ${r.status}`),
        ),
      );
    }
  }
  await Promise.all(creations);

  step(`open ${CLIENTS} connections concurrently, send/receive ${total} messages`);
  const started = Date.now();
  // All clients start at once so their AMQP connections race — this is what triggers the
  // unpatched listener's coalesced-header deadlock.
  const perClient = await withDeadline(Promise.all(Array.from({ length: CLIENTS }, (_, c) => runClient(c))), DEADLINE_MS);
  const received = perClient.reduce((a, b) => a + b, 0);
  const elapsed = ((Date.now() - started) / 1000).toFixed(1);
  check(received === total, `received all ${total} messages across ${CLIENTS} concurrent connections (${elapsed}s)`);

  step("delete queues");
  const deletions = [];
  for (let c = 0; c < CLIENTS; c++) {
    for (let l = 0; l < LINKS_PER_CLIENT; l++) {
      deletions.push(mgmt("DELETE", queueName(c, l)));
    }
  }
  await Promise.all(deletions);

  console.log("\nNode SDK concurrent-connection test passed");
}

main().catch((err) => {
  console.error("    FAIL:", err instanceof Error ? err.message : err);
  // Force exit: on the unpatched hang the stalled SDK connections keep the event loop alive, so
  // setting process.exitCode alone would never let the process terminate.
  process.exit(1);
});
