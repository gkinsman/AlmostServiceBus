package io.almostservicebus.smoke;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.ServiceBusSessionReceiverClient;
import com.azure.messaging.servicebus.models.DeadLetterOptions;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;
import com.azure.messaging.servicebus.models.SubQueue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Smoke test: the official Java Azure Service Bus SDK (azure-messaging-servicebus, proton-j
 * transport) against a running AlmostServiceBus emulator. Same scenario as the Python and Node
 * smoke tests (see ../README.md). Exits non-zero on the first failed check.
 *
 * <p>Entities are created through the emulator's plain-HTTP Atom management API with
 * java.net.http. The SDK's ServiceBusAdministrationClient cannot be used: it always speaks HTTPS
 * to the namespace host (ignoring the endpoint's port and the emulator flag) and the emulator,
 * like Microsoft's, is plaintext-only.
 *
 * <pre>ASB_CONNECTION_STRING=... ASB_ADMIN_ENDPOINT=http://localhost:5300 mvn -q compile exec:java</pre>
 */
public final class Smoke {

    private static final String CONNECTION_STRING = System.getenv().getOrDefault(
        "ASB_CONNECTION_STRING",
        "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;"
            + "SharedAccessKey=emulator;UseDevelopmentEmulator=true");
    private static final String ADMIN_ENDPOINT = System.getenv()
        .getOrDefault("ASB_ADMIN_ENDPOINT", "http://localhost:5300").replaceAll("/+$", "");
    private static final String RUN = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private static final String QUEUE = "smoke-java-" + RUN;
    private static final String SESSION_QUEUE = "smoke-java-sessions-" + RUN;
    private static final String TOPIC = "smoke-java-topic-" + RUN;
    private static final String SUBSCRIPTION = "worker";
    private static final Duration WAIT = Duration.ofSeconds(10);
    private static final String SB_NS = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";

    private static final HttpClient HTTP = HttpClient.newHttpClient();
    private static int stepNo = 0;

    private static void step(String name) {
        System.out.printf("[%02d] %s%n", ++stepNo, name);
    }

    private static void check(boolean cond, String what) {
        if (!cond) {
            System.out.println("    FAIL: " + what);
            System.exit(1);
        }
        System.out.println("    ok   " + what);
    }

    // ── plain-HTTP management helpers ────────────────────────────────────────

    private static String keyName() {
        for (String part : CONNECTION_STRING.split(";")) {
            if (part.startsWith("SharedAccessKeyName=")) return part.substring("SharedAccessKeyName=".length());
        }
        return "RootManageSharedAccessKey";
    }

    private static HttpResponse<String> mgmt(String method, String path, String body) throws Exception {
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(ADMIN_ENDPOINT + "/" + path + "?api-version=2021-05"))
            // The emulator maps SharedAccessKeyName to a namespace; the signature is not checked.
            .header("Authorization", "SharedAccessSignature sr=localhost&sig=x&se=1&skn=" + keyName())
            .header("Content-Type", "application/atom+xml;type=entry;charset=utf-8")
            .timeout(Duration.ofSeconds(10));
        b = body == null ? b.method(method, HttpRequest.BodyPublishers.noBody())
                         : b.method(method, HttpRequest.BodyPublishers.ofString(body));
        return HTTP.send(b.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static String entry(String descriptionTag, String inner) {
        return "<entry xmlns=\"http://www.w3.org/2005/Atom\"><content type=\"application/xml\">"
            + "<" + descriptionTag + " xmlns=\"" + SB_NS + "\" xmlns:i=\"http://www.w3.org/2001/XMLSchema-instance\">"
            + inner + "</" + descriptionTag + "></content></entry>";
    }

    private static boolean created(HttpResponse<?> r) {
        return r.statusCode() == 200 || r.statusCode() == 201;
    }

    // ── SDK helpers ──────────────────────────────────────────────────────────

    private static ServiceBusReceivedMessage receiveOne(ServiceBusReceiverClient receiver, Duration wait) {
        List<ServiceBusReceivedMessage> msgs = new ArrayList<>();
        receiver.receiveMessages(1, wait).forEach(msgs::add);
        check(msgs.size() == 1, "received one message (got " + msgs.size() + ")");
        return msgs.get(0);
    }

    private static ServiceBusReceivedMessage receiveOne(ServiceBusReceiverClient receiver) {
        return receiveOne(receiver, WAIT);
    }

    private static ServiceBusClientBuilder client() {
        return new ServiceBusClientBuilder().connectionString(CONNECTION_STRING);
    }

    public static void main(String[] args) throws Exception {
        step("management (plain HTTP Atom API): create queue, session queue, topic, subscription");
        HttpResponse<String> r = mgmt("PUT", QUEUE, entry("QueueDescription",
            "<LockDuration>PT1M</LockDuration><MaxDeliveryCount>3</MaxDeliveryCount>"));
        check(created(r), "create queue -> " + r.statusCode());
        r = mgmt("GET", QUEUE, null);
        check(r.statusCode() == 200 && r.body().contains("<LockDuration>PT1M</LockDuration>"), "get queue echoes LockDuration");
        check(r.body().contains("<MaxDeliveryCount>3</MaxDeliveryCount>"), "get queue echoes MaxDeliveryCount");
        r = mgmt("PUT", SESSION_QUEUE, entry("QueueDescription", "<RequiresSession>true</RequiresSession>"));
        check(created(r), "create session queue -> " + r.statusCode());
        r = mgmt("PUT", TOPIC, entry("TopicDescription", ""));
        check(created(r), "create topic -> " + r.statusCode());
        r = mgmt("PUT", TOPIC + "/subscriptions/" + SUBSCRIPTION, entry("SubscriptionDescription", ""));
        check(created(r), "create subscription -> " + r.statusCode());
        r = mgmt("GET", "$Resources/queues", null);
        check(r.statusCode() == 200 && r.body().contains(QUEUE), "list queues includes the queue");

        step("send with application properties, receive (PeekLock), complete");
        try (ServiceBusSenderClient sender = client().sender().queueName(QUEUE).buildClient();
             ServiceBusReceiverClient receiver = client().receiver().queueName(QUEUE)
                 .receiveMode(ServiceBusReceiveMode.PEEK_LOCK).disableAutoComplete().buildClient()) {

            ServiceBusMessage out = new ServiceBusMessage("{\"orderId\":\"1\"}")
                .setMessageId("msg-1").setSubject("OrderPlaced").setCorrelationId("corr-1").setContentType("application/json");
            out.getApplicationProperties().put("source", "smoke-java");
            out.getApplicationProperties().put("attempt", 1);
            sender.sendMessage(out);

            ServiceBusReceivedMessage m = receiveOne(receiver);
            check("{\"orderId\":\"1\"}".equals(m.getBody().toString()), "body round-trips");
            check("msg-1".equals(m.getMessageId()), "messageId round-trips (" + m.getMessageId() + ")");
            check("OrderPlaced".equals(m.getSubject()), "subject round-trips (" + m.getSubject() + ")");
            check("corr-1".equals(m.getCorrelationId()), "correlationId round-trips (" + m.getCorrelationId() + ")");
            check("application/json".equals(m.getContentType()), "contentType round-trips (" + m.getContentType() + ")");
            check("smoke-java".equals(m.getApplicationProperties().get("source")), "string application property round-trips (" + m.getApplicationProperties().get("source") + ")");
            check(Integer.valueOf(1).equals(m.getApplicationProperties().get("attempt")), "int application property round-trips (" + m.getApplicationProperties().get("attempt") + ")");
            long firstDeliveryCount = m.getDeliveryCount();
            check(m.getSequenceNumber() > 0, "sequenceNumber assigned (" + m.getSequenceNumber() + ")");
            check(m.getLockedUntil() != null, "lockedUntil set");

            step("renew lock");
            OffsetDateTime before = m.getLockedUntil();
            Thread.sleep(1100);
            OffsetDateTime after = receiver.renewMessageLock(m);
            check(after.isAfter(before), "lockedUntil moved forward (" + before + " -> " + after + ")");
            receiver.complete(m);

            step("peek");
            sender.sendMessages(List.of(new ServiceBusMessage("p1"), new ServiceBusMessage("p2")));
            List<ServiceBusReceivedMessage> peeked = new ArrayList<>();
            receiver.peekMessages(5).forEach(peeked::add);
            check(peeked.size() == 2, "peek sees both messages (" + peeked.size() + ")");
            check(peeked.get(0).getSequenceNumber() < peeked.get(1).getSequenceNumber(), "peek is in sequence order");
            for (int i = 0; i < 2; i++) receiver.complete(receiveOne(receiver));
            List<ServiceBusReceivedMessage> empty = new ArrayList<>();
            receiver.peekMessages(5).forEach(empty::add);
            check(empty.isEmpty(), "queue empty after completing both");

            step("abandon -> redelivery bumps deliveryCount");
            sender.sendMessage(new ServiceBusMessage("retry-me"));
            ServiceBusReceivedMessage first = receiveOne(receiver);
            receiver.abandon(first);
            ServiceBusReceivedMessage second = receiveOne(receiver);
            check("retry-me".equals(second.getBody().toString()), "abandoned message is redelivered");
            check(second.getDeliveryCount() == firstDeliveryCount + 1,
                "deliveryCount is one higher after abandon (" + firstDeliveryCount + " -> " + second.getDeliveryCount() + ")");
            receiver.complete(second);

            step("dead-letter with reason, then receive from the dead-letter sub-queue");
            sender.sendMessage(new ServiceBusMessage("poison").setMessageId("poison-1"));
            ServiceBusReceivedMessage bad = receiveOne(receiver);
            receiver.deadLetter(bad, new DeadLetterOptions().setDeadLetterReason("ValidationFailed").setDeadLetterErrorDescription("schema mismatch"));
            try (ServiceBusReceiverClient dlq = client().receiver().queueName(QUEUE)
                     .subQueue(SubQueue.DEAD_LETTER_QUEUE).disableAutoComplete().buildClient()) {
                ServiceBusReceivedMessage dead = receiveOne(dlq);
                check("poison-1".equals(dead.getMessageId()), "dead-lettered message is in the DLQ");
                check("ValidationFailed".equals(dead.getDeadLetterReason()), "deadLetterReason (" + dead.getDeadLetterReason() + ")");
                check("schema mismatch".equals(dead.getDeadLetterErrorDescription()), "deadLetterErrorDescription (" + dead.getDeadLetterErrorDescription() + ")");
                dlq.complete(dead);
            }

            step("scheduled message is not delivered early");
            long scheduledAt = System.nanoTime();
            long seq = sender.scheduleMessage(new ServiceBusMessage("later"), OffsetDateTime.now().plusSeconds(3));
            check(seq > 0, "scheduleMessage returns a sequence number (" + seq + ")");
            List<ServiceBusReceivedMessage> early = new ArrayList<>();
            receiver.receiveMessages(1, Duration.ofSeconds(1)).forEach(early::add);
            check(early.isEmpty(), "nothing arrives in the first second");
            ServiceBusReceivedMessage late = receiveOne(receiver);
            double elapsed = (System.nanoTime() - scheduledAt) / 1e9;
            check("later".equals(late.getBody().toString()), "scheduled message arrives");
            check(elapsed >= 2.5, String.format("...and not before the scheduled time (%.1fs after scheduling)", elapsed));
            receiver.complete(late);
        }

        step("sessions: FIFO within a session, session state, next-available session");
        try (ServiceBusSenderClient sender = client().sender().queueName(SESSION_QUEUE).buildClient()) {
            List<ServiceBusMessage> batch = new ArrayList<>();
            for (int i = 0; i < 3; i++) batch.add(new ServiceBusMessage("s1-" + i).setSessionId("s1"));
            sender.sendMessages(batch);
            sender.sendMessage(new ServiceBusMessage("s2-0").setSessionId("s2"));
        }
        try (ServiceBusSessionReceiverClient sessions = client().sessionReceiver().queueName(SESSION_QUEUE)
                 .disableAutoComplete().buildClient()) {
            try (ServiceBusReceiverClient s1 = sessions.acceptSession("s1")) {
                check("s1".equals(s1.getSessionId()), "accepted the requested session");
                List<String> got = new ArrayList<>();
                List<ServiceBusReceivedMessage> held = new ArrayList<>();
                s1.receiveMessages(3, WAIT).forEach(x -> { got.add(x.getBody().toString()); held.add(x); });
                check(List.of("s1-0", "s1-1", "s1-2").equals(got), "session messages arrive in order (" + got + ")");
                s1.setSessionState("checkpoint-3".getBytes(StandardCharsets.UTF_8));
                check("checkpoint-3".equals(new String(s1.getSessionState(), StandardCharsets.UTF_8)), "session state round-trips");
                s1.renewSessionLock();
                for (ServiceBusReceivedMessage x : held) s1.complete(x);
            }
            try (ServiceBusReceiverClient next = sessions.acceptNextSession()) {
                check("s2".equals(next.getSessionId()), "next-available session is s2 (" + next.getSessionId() + ")");
                ServiceBusReceivedMessage s2 = receiveOne(next);
                check("s2-0".equals(s2.getBody().toString()), "s2 message received");
                next.complete(s2);
            }
        }

        step("topic -> subscription");
        try (ServiceBusSenderClient sender = client().sender().topicName(TOPIC).buildClient();
             ServiceBusReceiverClient sub = client().receiver().topicName(TOPIC).subscriptionName(SUBSCRIPTION)
                 .disableAutoComplete().buildClient()) {
            ServiceBusMessage out = new ServiceBusMessage("via-topic");
            out.getApplicationProperties().put("k", "v");
            sender.sendMessage(out);
            ServiceBusReceivedMessage t = receiveOne(sub);
            check("via-topic".equals(t.getBody().toString()), "subscription receives the published message");
            check("v".equals(t.getApplicationProperties().get("k")), "application property survives topic fan-out");
            sub.complete(t);
        }

        step("management (plain HTTP Atom API): delete entities");
        for (String path : List.of(QUEUE, SESSION_QUEUE, TOPIC)) {
            r = mgmt("DELETE", path, null);
            check(r.statusCode() == 200 || r.statusCode() == 204, "delete " + path + " -> " + r.statusCode());
        }
        r = mgmt("GET", QUEUE, null);
        check(r.statusCode() == 404, "deleted queue is gone (404)");

        System.out.println("\nJava SDK smoke test passed");
        System.exit(0);
    }
}
