"""
Smoke test: the official Python Azure Service Bus SDK (`azure-servicebus`, pyamqp transport)
against a running AlmostServiceBus emulator.

Same scenario as the Node and Java smoke tests (see ../README.md): send/receive/complete with
application properties, peek, lock renewal, abandon + redelivery, dead-letter with reason and
DLQ receive, sessions with session state, topic -> subscription, scheduled messages.

Entities are created through the emulator's plain-HTTP Atom management API using urllib. The
SDK's own ServiceBusAdministrationClient cannot be used: it hard-codes https:// and the
emulator (like Microsoft's) is plaintext-only. The .NET SDK is the only one whose admin client
honours UseDevelopmentEmulator=true.

    ASB_CONNECTION_STRING=... ASB_ADMIN_ENDPOINT=http://localhost:5300 python smoke.py

Exits non-zero on the first failed check.
"""

import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

from azure.servicebus import (
    NEXT_AVAILABLE_SESSION,
    ServiceBusClient,
    ServiceBusMessage,
    ServiceBusReceiveMode,
    ServiceBusSubQueue,
)

CONNECTION_STRING = os.environ.get(
    "ASB_CONNECTION_STRING",
    "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;"
    "SharedAccessKey=emulator;UseDevelopmentEmulator=true",
)
ADMIN_ENDPOINT = os.environ.get("ASB_ADMIN_ENDPOINT", "http://localhost:5300").rstrip("/")
RUN = uuid.uuid4().hex[:8]
QUEUE = f"smoke-py-{RUN}"
SESSION_QUEUE = f"smoke-py-sessions-{RUN}"
TOPIC = f"smoke-py-topic-{RUN}"
SUBSCRIPTION = "worker"
WAIT = 10  # seconds to wait for a message that should already be there

SB_NS = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect"

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


# ── plain-HTTP management helpers ──────────────────────────────────────────────

def _key_name():
    for part in CONNECTION_STRING.split(";"):
        if part.startswith("SharedAccessKeyName="):
            return part.split("=", 1)[1]
    return "RootManageSharedAccessKey"


def mgmt(method, path, body=None):
    req = urllib.request.Request(
        f"{ADMIN_ENDPOINT}/{path}?api-version=2021-05",
        data=body.encode() if body else None,
        method=method,
        headers={
            # The emulator maps SharedAccessKeyName to a namespace; the signature is not checked.
            "Authorization": f"SharedAccessSignature sr=localhost&sig=x&se=1&skn={_key_name()}",
            "Content-Type": "application/atom+xml;type=entry;charset=utf-8",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def entry(description_tag, inner_xml):
    return (
        '<entry xmlns="http://www.w3.org/2005/Atom"><content type="application/xml">'
        f'<{description_tag} xmlns="{SB_NS}" xmlns:i="http://www.w3.org/2001/XMLSchema-instance">'
        f"{inner_xml}</{description_tag}></content></entry>"
    )


# ── SDK helpers ────────────────────────────────────────────────────────────────

def prop(msg, key):
    """pyamqp hands back application property keys as bytes; accept either."""
    props = msg.application_properties or {}
    return props.get(key, props.get(key.encode()))


def text(value):
    return value.decode() if isinstance(value, (bytes, bytearray)) else value


def body_text(msg):
    return text(b"".join(msg.body)) if not isinstance(msg.body, str) else msg.body


def receive_one(receiver, wait=WAIT):
    msgs = receiver.receive_messages(max_message_count=1, max_wait_time=wait)
    check(len(msgs) == 1, f"received one message (got {len(msgs)})")
    return msgs[0]


def main():
    step("management (plain HTTP Atom API): create queue, session queue, topic, subscription")
    status, xml = mgmt("PUT", QUEUE, entry("QueueDescription",
        "<LockDuration>PT1M</LockDuration><MaxDeliveryCount>3</MaxDeliveryCount>"))
    check(status in (200, 201), f"create queue -> {status}")
    status, xml = mgmt("GET", QUEUE)
    check(status == 200 and "<LockDuration>PT1M</LockDuration>" in xml, "get queue echoes LockDuration")
    check("<MaxDeliveryCount>3</MaxDeliveryCount>" in xml, "get queue echoes MaxDeliveryCount")
    status, _ = mgmt("PUT", SESSION_QUEUE, entry("QueueDescription", "<RequiresSession>true</RequiresSession>"))
    check(status in (200, 201), f"create session queue -> {status}")
    status, _ = mgmt("PUT", TOPIC, entry("TopicDescription", ""))
    check(status in (200, 201), f"create topic -> {status}")
    status, _ = mgmt("PUT", f"{TOPIC}/subscriptions/{SUBSCRIPTION}", entry("SubscriptionDescription", ""))
    check(status in (200, 201), f"create subscription -> {status}")
    status, xml = mgmt("GET", "$Resources/queues")
    check(status == 200 and QUEUE in xml, "list queues includes the queue")

    with ServiceBusClient.from_connection_string(CONNECTION_STRING) as client:
        step("send with application properties, receive (PeekLock), complete")
        with client.get_queue_sender(QUEUE) as sender:
            sender.send_messages(
                ServiceBusMessage(
                    '{"orderId":"1"}',
                    application_properties={"source": "smoke-py", "attempt": 1},
                    subject="OrderPlaced",
                    correlation_id="corr-1",
                    content_type="application/json",
                    message_id="msg-1",
                )
            )
        with client.get_queue_receiver(QUEUE, receive_mode=ServiceBusReceiveMode.PEEK_LOCK) as receiver:
            m = receive_one(receiver)
            check(body_text(m) == '{"orderId":"1"}', "body round-trips")
            check(m.message_id == "msg-1", f"message_id round-trips ({m.message_id})")
            check(m.subject == "OrderPlaced", f"subject round-trips ({m.subject})")
            check(m.correlation_id == "corr-1", f"correlation_id round-trips ({m.correlation_id})")
            check(m.content_type == "application/json", f"content_type round-trips ({m.content_type})")
            check(text(prop(m, "source")) == "smoke-py", f"string application property round-trips ({prop(m, 'source')!r})")
            check(prop(m, "attempt") == 1, f"int application property round-trips ({prop(m, 'attempt')!r})")
            # pyamqp exposes the raw AMQP header delivery-count (0 on first delivery); .NET adds one.
            first_delivery_count = m.delivery_count
            check(m.sequence_number is not None and m.sequence_number > 0, f"sequence_number assigned ({m.sequence_number})")
            check(m.locked_until_utc is not None, "locked_until_utc set")

            step("renew lock")
            before = m.locked_until_utc
            time.sleep(1.1)
            after = receiver.renew_message_lock(m)
            check(after > before, f"locked_until moved forward ({before} -> {after})")
            receiver.complete_message(m)

            step("peek")
            with client.get_queue_sender(QUEUE) as sender:
                sender.send_messages([ServiceBusMessage("p1"), ServiceBusMessage("p2")])
            peeked = receiver.peek_messages(max_message_count=5)
            check(len(peeked) == 2, f"peek sees both messages ({len(peeked)})")
            check(peeked[0].sequence_number < peeked[1].sequence_number, "peek is in sequence order")
            for _ in range(2):
                receiver.complete_message(receive_one(receiver))
            check(len(receiver.peek_messages(max_message_count=5)) == 0, "queue empty after completing both")

            step("batch send where the first message has a subject")
            with client.get_queue_sender(QUEUE) as sender:
                sender.send_messages([
                    ServiceBusMessage("b1", subject="OrderPlaced", application_properties={"n": 1}),
                    ServiceBusMessage("b2", subject="OrderPlaced", application_properties={"n": 2}),
                    ServiceBusMessage("b3", subject="OrderShipped", application_properties={"n": 3}),
                ])
            batch = receiver.receive_messages(max_message_count=5, max_wait_time=WAIT)
            check(len(batch) == 3, f"all three batched messages arrive as separate messages ({len(batch)})")
            check([str(m) for m in batch] == ["b1", "b2", "b3"], f"bodies intact ({[str(m) for m in batch]})")
            check(batch[2].subject == "OrderShipped" and batch[2].application_properties[b"n"] == 3, "each message keeps its own subject and properties")
            for m in batch:
                receiver.complete_message(m)

            step("abandon -> redelivery bumps delivery_count")
            with client.get_queue_sender(QUEUE) as sender:
                sender.send_messages(ServiceBusMessage("retry-me"))
            first = receive_one(receiver)
            receiver.abandon_message(first)
            second = receive_one(receiver)
            check(body_text(second) == "retry-me", "abandoned message is redelivered")
            check(second.delivery_count == first_delivery_count + 1,
                  f"delivery_count is one higher after abandon ({first_delivery_count} -> {second.delivery_count})")
            receiver.complete_message(second)

            step("dead-letter with reason, then receive from the dead-letter sub-queue")
            with client.get_queue_sender(QUEUE) as sender:
                sender.send_messages(ServiceBusMessage("poison", message_id="poison-1"))
            bad = receive_one(receiver)
            receiver.dead_letter_message(bad, reason="ValidationFailed", error_description="schema mismatch")

        with client.get_queue_receiver(QUEUE, sub_queue=ServiceBusSubQueue.DEAD_LETTER) as dlq:
            dead = receive_one(dlq)
            check(dead.message_id == "poison-1", "dead-lettered message is in the DLQ")
            check(dead.dead_letter_reason == "ValidationFailed", f"dead_letter_reason ({dead.dead_letter_reason})")
            check(dead.dead_letter_error_description == "schema mismatch", f"dead_letter_error_description ({dead.dead_letter_error_description})")
            dlq.complete_message(dead)

        step("sessions: FIFO within a session, session state, next-available session")
        with client.get_queue_sender(SESSION_QUEUE) as sender:
            sender.send_messages([ServiceBusMessage(f"s1-{i}", session_id="s1") for i in range(3)])
            sender.send_messages(ServiceBusMessage("s2-0", session_id="s2"))
        with client.get_queue_receiver(SESSION_QUEUE, session_id="s1") as sr:
            check(sr.session.session_id == "s1", "accepted the requested session")
            held = sr.receive_messages(max_message_count=3, max_wait_time=WAIT)
            got = [body_text(m) for m in held]
            check(got == ["s1-0", "s1-1", "s1-2"], f"session messages arrive in order ({got})")
            sr.session.set_state(b"checkpoint-3")
            check(text(sr.session.get_state()) == "checkpoint-3", "session state round-trips")
            sr.session.renew_lock()
            for m in held:
                sr.complete_message(m)
        with client.get_queue_receiver(SESSION_QUEUE, session_id=NEXT_AVAILABLE_SESSION) as sr:
            check(sr.session.session_id == "s2", f"next-available session is s2 ({sr.session.session_id})")
            m = receive_one(sr)
            check(body_text(m) == "s2-0", "s2 message received")
            sr.complete_message(m)

        step("topic -> subscription")
        with client.get_topic_sender(TOPIC) as sender:
            sender.send_messages(ServiceBusMessage("via-topic", application_properties={"k": "v"}))
        with client.get_subscription_receiver(TOPIC, SUBSCRIPTION) as sub:
            m = receive_one(sub)
            check(body_text(m) == "via-topic", "subscription receives the published message")
            check(text(prop(m, "k")) == "v", "application property survives topic fan-out")
            sub.complete_message(m)

        step("scheduled message is not delivered early")
        with client.get_queue_sender(QUEUE) as sender:
            when = datetime.now(timezone.utc) + timedelta(seconds=3)
            scheduled_at = time.monotonic()
            seqs = sender.schedule_messages(ServiceBusMessage("later"), when)
            check(len(seqs) == 1 and seqs[0] > 0, f"schedule_messages returns a sequence number ({seqs})")
        with client.get_queue_receiver(QUEUE) as receiver:
            check(len(receiver.receive_messages(max_message_count=1, max_wait_time=1)) == 0, "nothing arrives in the first second")
            m = receive_one(receiver, wait=WAIT)
            elapsed = time.monotonic() - scheduled_at
            check(body_text(m) == "later", "scheduled message arrives")
            check(elapsed >= 2.5, f"...and not before the scheduled time ({elapsed:.1f}s after scheduling)")
            receiver.complete_message(m)

    step("management (plain HTTP Atom API): delete entities")
    for path in (QUEUE, SESSION_QUEUE, TOPIC):
        status, _ = mgmt("DELETE", path)
        check(status in (200, 204), f"delete {path} -> {status}")
    status, _ = mgmt("GET", QUEUE)
    check(status == 404, "deleted queue is gone (404)")

    print("\nPython SDK smoke test passed", flush=True)


if __name__ == "__main__":
    main()
