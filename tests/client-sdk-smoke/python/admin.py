"""
Admin-interface test: the official Python Azure Service Bus SDK's
ServiceBusAdministrationClient against a running AlmostServiceBus emulator over its HTTPS admin
endpoint (default port 5301).

Unlike smoke.py -- which drives the data plane and creates entities through the plain-HTTP Atom
API -- this exercises the SDK admin client itself: queue / topic / subscription / rule create,
get, list, update and delete, plus a data-plane round-trip on an admin-created queue to prove the
entity is usable.

The admin client speaks HTTPS to the endpoint host and port, so it needs to trust the emulator's
CA. The `requests` transport honours the REQUESTS_CA_BUNDLE environment variable (it ignores
SSL_CERT_FILE); point it at the emitted emulator-ca.crt. See ../../../certs/README.md.

    REQUESTS_CA_BUNDLE=<certdir>/emulator-ca.crt \
    ASB_ADMIN_CONNECTION_STRING=Endpoint=sb://localhost:5301;...;UseDevelopmentEmulator=true \
    python admin.py

Exits non-zero on the first failed check.
"""

import os
import sys
import uuid

from azure.servicebus import ServiceBusClient, ServiceBusMessage, ServiceBusReceiveMode
from azure.servicebus.management import ServiceBusAdministrationClient

ADMIN_CONNECTION_STRING = os.environ.get(
    "ASB_ADMIN_CONNECTION_STRING",
    "Endpoint=sb://localhost:5301;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=emulator;UseDevelopmentEmulator=true",
)
DATA_CONNECTION_STRING = os.environ.get(
    "ASB_CONNECTION_STRING",
    "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=emulator;UseDevelopmentEmulator=true",
)

RUN = uuid.uuid4().hex[:8]
QUEUE = f"admin-py-{RUN}"
TOPIC = f"admin-py-topic-{RUN}"
SUBSCRIPTION = "worker"
RULE = "high-priority"

_step = 0


def step(name):
    global _step
    _step += 1
    print(f"[{_step:02d}] {name}", flush=True)


def check(cond, what):
    if not cond:
        print(f"    FAIL: {what}", flush=True)
        sys.exit(1)
    print(f"    ok   {what}", flush=True)


def body_text(msg):
    body = msg.body
    if isinstance(body, str):
        return body
    return b"".join(body).decode()


def main():
    admin = ServiceBusAdministrationClient.from_connection_string(ADMIN_CONNECTION_STRING)

    step("create queue with options")
    queue = admin.create_queue(
        QUEUE,
        lock_duration="PT1M",
        max_delivery_count=5,
        max_size_in_megabytes=2048,
        dead_lettering_on_message_expiration=True,
        enable_batched_operations=True,
    )
    check(queue.name == QUEUE, f"queue created ({queue.name})")
    check(queue.max_delivery_count == 5, f"maxDeliveryCount set ({queue.max_delivery_count})")

    step("get queue and confirm options round-trip")
    got_queue = admin.get_queue(QUEUE)
    check(got_queue.max_delivery_count == 5, f"maxDeliveryCount round-trips ({got_queue.max_delivery_count})")
    check(got_queue.max_size_in_megabytes == 2048, f"maxSizeInMegabytes round-trips ({got_queue.max_size_in_megabytes})")
    check(got_queue.dead_lettering_on_message_expiration is True, "deadLetteringOnMessageExpiration round-trips")

    step("update queue")
    got_queue.max_delivery_count = 8
    admin.update_queue(got_queue)
    check(admin.get_queue(QUEUE).max_delivery_count == 8, "updated maxDeliveryCount persisted")

    step("create topic and confirm")
    topic = admin.create_topic(TOPIC, enable_batched_operations=True)
    check(topic.name == TOPIC, f"topic created ({topic.name})")

    step("create subscription with options")
    subscription = admin.create_subscription(
        TOPIC,
        SUBSCRIPTION,
        lock_duration="PT45S",
        max_delivery_count=4,
        dead_lettering_on_message_expiration=True,
    )
    check(subscription.name == SUBSCRIPTION, f"subscription created ({subscription.name})")
    check(subscription.max_delivery_count == 4, f"subscription maxDeliveryCount ({subscription.max_delivery_count})")

    step("get subscription and confirm round-trip")
    got_sub = admin.get_subscription(TOPIC, SUBSCRIPTION)
    check(got_sub.lock_duration is not None, "subscription lockDuration present")

    step("create SQL rule")
    from azure.servicebus.management import SqlRuleFilter

    admin.create_rule(TOPIC, SUBSCRIPTION, RULE, filter=SqlRuleFilter("priority = 'high'"))
    rules = list(admin.list_rules(TOPIC, SUBSCRIPTION))
    check(any(r.name == RULE for r in rules), f"rule created ({RULE})")

    step("list entities")
    check(len(list(admin.list_queues())) >= 1, "listQueues returns the queue")
    check(len(list(admin.list_topics())) >= 1, "listTopics returns the topic")
    check(len(list(admin.list_subscriptions(TOPIC))) == 1, "listSubscriptions returns one subscription")
    # A subscription always keeps its implicit $Default rule alongside the one we added.
    check(len(rules) == 2, "listRules returns $Default + our rule")

    step("usage: send and receive on the admin-created queue (data plane)")
    with ServiceBusClient.from_connection_string(DATA_CONNECTION_STRING) as client:
        with client.get_queue_sender(QUEUE) as sender:
            sender.send_messages(ServiceBusMessage("hello-admin", subject="AdminCreated"))
        with client.get_queue_receiver(QUEUE, receive_mode=ServiceBusReceiveMode.PEEK_LOCK) as receiver:
            msgs = receiver.receive_messages(max_message_count=1, max_wait_time=10)
            check(len(msgs) == 1, f"received one message (got {len(msgs)})")
            check(body_text(msgs[0]) == "hello-admin", "message body round-trips")
            check(msgs[0].subject == "AdminCreated", f"subject round-trips ({msgs[0].subject})")
            receiver.complete_message(msgs[0])

    step("delete rule, subscription, topic, queue")
    admin.delete_rule(TOPIC, SUBSCRIPTION, RULE)
    admin.delete_subscription(TOPIC, SUBSCRIPTION)
    admin.delete_topic(TOPIC)
    admin.delete_queue(QUEUE)
    check(QUEUE not in [q.name for q in admin.list_queues()], "queue is gone after delete")
    check(TOPIC not in [t.name for t in admin.list_topics()], "topic is gone after delete")

    print("\nPython SDK admin test passed", flush=True)


if __name__ == "__main__":
    main()
