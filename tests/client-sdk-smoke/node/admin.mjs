// Admin-interface test: the official Node.js Azure Service Bus SDK's
// ServiceBusAdministrationClient against a running AlmostServiceBus emulator over its HTTPS
// admin endpoint (default port 5301). Unlike smoke.mjs — which drives the data plane and creates
// entities through the plain-HTTP Atom API — this exercises the SDK admin client itself:
// queue / topic / subscription / rule create, get, list, update and delete, plus a data-plane
// round-trip on an admin-created queue to prove the entity is usable.
//
// The admin client speaks HTTPS to the endpoint host and port, so it needs to trust the
// emulator's CA. Point NODE_EXTRA_CA_CERTS at the emitted emulator-ca.crt *before* starting node
// (Node reads it at startup and ignores the OS trust store). See ../../../certs/README.md.
//
//   NODE_EXTRA_CA_CERTS=<certdir>/emulator-ca.crt \
//   ASB_ADMIN_CONNECTION_STRING=Endpoint=sb://localhost:5301;...;UseDevelopmentEmulator=true \
//   node admin.mjs

import { ServiceBusAdministrationClient, ServiceBusClient } from "@azure/service-bus";
import { randomUUID } from "node:crypto";

const ADMIN_CONNECTION_STRING =
  process.env.ASB_ADMIN_CONNECTION_STRING ??
  "Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";
const DATA_CONNECTION_STRING =
  process.env.ASB_CONNECTION_STRING ??
  "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true";

const RUN = randomUUID().replace(/-/g, "").slice(0, 8);
const QUEUE = `admin-node-${RUN}`;
const TOPIC = `admin-node-topic-${RUN}`;
const SUBSCRIPTION = "worker";
const RULE = "high-priority";

let stepNo = 0;
const step = (name) => console.log(`[${String(++stepNo).padStart(2, "0")}] ${name}`);
function check(cond, what) {
  if (!cond) throw new Error(`check failed: ${what}`);
  console.log(`    ok   ${what}`);
}

async function count(iterable) {
  let n = 0;
  for await (const _ of iterable) n++;
  return n;
}

async function main() {
  const admin = new ServiceBusAdministrationClient(ADMIN_CONNECTION_STRING);

  step("create queue with options");
  let queue = await admin.createQueue(QUEUE, {
    lockDuration: "PT1M",
    maxDeliveryCount: 5,
    maxSizeInMegabytes: 2048,
    deadLetteringOnMessageExpiration: true,
    enableBatchedOperations: true,
  });
  check(queue.name === QUEUE, `queue created (${queue.name})`);
  check(queue.lockDuration === "PT1M", `lockDuration set (${queue.lockDuration})`);
  check(queue.maxDeliveryCount === 5, `maxDeliveryCount set (${queue.maxDeliveryCount})`);

  step("get queue and confirm options round-trip");
  const gotQueue = await admin.getQueue(QUEUE);
  check(gotQueue.maxDeliveryCount === 5, `maxDeliveryCount round-trips (${gotQueue.maxDeliveryCount})`);
  check(gotQueue.maxSizeInMegabytes === 2048, `maxSizeInMegabytes round-trips (${gotQueue.maxSizeInMegabytes})`);
  check(gotQueue.deadLetteringOnMessageExpiration === true, "deadLetteringOnMessageExpiration round-trips");
  check(await admin.queueExists(QUEUE), "queueExists reports true");

  step("update queue");
  gotQueue.maxDeliveryCount = 8;
  const updatedQueue = await admin.updateQueue(gotQueue);
  check(updatedQueue.maxDeliveryCount === 8, `updated maxDeliveryCount (${updatedQueue.maxDeliveryCount})`);

  step("create topic and confirm");
  const topic = await admin.createTopic(TOPIC, { enableBatchedOperations: true });
  check(topic.name === TOPIC, `topic created (${topic.name})`);
  check(await admin.topicExists(TOPIC), "topicExists reports true");

  step("create subscription with options");
  const subscription = await admin.createSubscription(TOPIC, SUBSCRIPTION, {
    lockDuration: "PT45S",
    maxDeliveryCount: 4,
    deadLetteringOnMessageExpiration: true,
  });
  check(subscription.topicName === TOPIC, `subscription topic (${subscription.topicName})`);
  check(subscription.subscriptionName === SUBSCRIPTION, `subscription name (${subscription.subscriptionName})`);
  check(subscription.maxDeliveryCount === 4, `subscription maxDeliveryCount (${subscription.maxDeliveryCount})`);

  step("get subscription and confirm round-trip");
  const gotSub = await admin.getSubscription(TOPIC, SUBSCRIPTION);
  check(gotSub.lockDuration === "PT45S", `subscription lockDuration round-trips (${gotSub.lockDuration})`);
  check(await admin.subscriptionExists(TOPIC, SUBSCRIPTION), "subscriptionExists reports true");

  step("create SQL rule");
  const rule = await admin.createRule(TOPIC, SUBSCRIPTION, RULE, {
    sqlExpression: "priority = 'high'",
  });
  check(rule.name === RULE, `rule created (${rule.name})`);

  step("list entities");
  check((await count(admin.listQueues())) >= 1, "listQueues returns the queue");
  check((await count(admin.listTopics())) >= 1, "listTopics returns the topic");
  check((await count(admin.listSubscriptions(TOPIC))) === 1, "listSubscriptions returns one subscription");
  // A subscription always keeps its implicit $Default rule alongside the one we added.
  check((await count(admin.listRules(TOPIC, SUBSCRIPTION))) === 2, "listRules returns $Default + our rule");

  step("usage: send and receive on the admin-created queue (data plane)");
  const client = new ServiceBusClient(DATA_CONNECTION_STRING);
  try {
    const sender = client.createSender(QUEUE);
    await sender.sendMessages({ body: { hello: "admin" }, subject: "AdminCreated" });
    const receiver = client.createReceiver(QUEUE, { receiveMode: "peekLock" });
    const msgs = await receiver.receiveMessages(1, { maxWaitTimeInMs: 10_000 });
    check(msgs.length === 1, `received one message (got ${msgs.length})`);
    check(msgs[0].body?.hello === "admin", "message body round-trips");
    check(msgs[0].subject === "AdminCreated", `subject round-trips (${msgs[0].subject})`);
    await receiver.completeMessage(msgs[0]);
  } finally {
    await client.close();
  }

  step("delete rule, subscription, topic, queue");
  await admin.deleteRule(TOPIC, SUBSCRIPTION, RULE);
  await admin.deleteSubscription(TOPIC, SUBSCRIPTION);
  await admin.deleteTopic(TOPIC);
  await admin.deleteQueue(QUEUE);
  check(!(await admin.queueExists(QUEUE)), "queue is gone after delete");
  check(!(await admin.topicExists(TOPIC)), "topic is gone after delete");

  console.log("\nNode.js SDK admin test passed");
}

main().catch((err) => {
  console.error(`\nFAILED: ${err.message}`);
  process.exit(1);
});
