package io.almostservicebus.smoke;

import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Admin-interface test: exercises the AlmostServiceBus emulator's management (admin) interface
 * over its HTTPS admin endpoint (default port 5301) from Java, covering queue / topic /
 * subscription / rule create, get, list and delete, plus a data-plane round-trip on an
 * admin-created queue to prove the entity is usable.
 *
 * <p>Unlike Node and Python, the Java {@code ServiceBusAdministrationClient} cannot be pointed at
 * the emulator's admin endpoint: it strips the port and always speaks HTTPS to the namespace host
 * on 443, ignoring both the connection-string port and an explicit {@code .endpoint(...)} override.
 * So this test drives the same Atom REST API the SDK admin client uses, over HTTPS on 5301, with
 * {@code java.net.http}. Java trusts the emulator CA through a PKCS12 truststore passed as system
 * properties (see ../../../certs/README.md):
 *
 * <pre>
 * java -Djavax.net.ssl.trustStore=&lt;certdir&gt;/emulator-truststore.p12 \
 *      -Djavax.net.ssl.trustStoreType=PKCS12 \
 *      -Djavax.net.ssl.trustStorePassword=changeit ...
 * </pre>
 *
 * <p>The data-plane round-trip uses the SDK against the plain-AMQP endpoint on 5672, which does
 * honour {@code UseDevelopmentEmulator=true}. Exits non-zero on the first failed check.
 */
public final class AdminSmoke {

    private static final String CONNECTION_STRING = System.getenv().getOrDefault(
        "ASB_CONNECTION_STRING",
        "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;"
            + "SharedAccessKey=emulator;UseDevelopmentEmulator=true");

    // HTTPS admin endpoint (the TLS-terminating Kestrel listener, default port 5301).
    private static final String ADMIN_ENDPOINT = System.getenv()
        .getOrDefault("ASB_ADMIN_TLS_ENDPOINT", "https://localhost:5301").replaceAll("/+$", "");

    private static final String SB_NS = "http://schemas.microsoft.com/netservices/2010/10/servicebus/connect";
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String RUN = UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    private static final String QUEUE = "admin-java-" + RUN;
    private static final String TOPIC = "admin-java-topic-" + RUN;
    private static final String SUBSCRIPTION = "worker";
    private static final String RULE = "high-priority";

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

    public static void main(String[] args) throws Exception {
        step("create queue with options (HTTPS admin API on " + ADMIN_ENDPOINT + ")");
        HttpResponse<String> r = mgmt("PUT", QUEUE, entry("QueueDescription",
            "<LockDuration>PT1M</LockDuration><MaxDeliveryCount>5</MaxDeliveryCount>"
                + "<MaxSizeInMegabytes>2048</MaxSizeInMegabytes>"
                + "<DeadLetteringOnMessageExpiration>true</DeadLetteringOnMessageExpiration>"));
        check(created(r), "create queue -> " + r.statusCode());

        step("get queue and confirm options round-trip");
        r = mgmt("GET", QUEUE, null);
        check(r.statusCode() == 200, "get queue -> " + r.statusCode());
        check(r.body().contains("<LockDuration>PT1M</LockDuration>"), "get queue echoes LockDuration");
        check(r.body().contains("<MaxDeliveryCount>5</MaxDeliveryCount>"), "get queue echoes MaxDeliveryCount");
        check(r.body().contains("<MaxSizeInMegabytes>2048</MaxSizeInMegabytes>"), "get queue echoes MaxSizeInMegabytes");

        step("create topic");
        r = mgmt("PUT", TOPIC, entry("TopicDescription", ""));
        check(created(r), "create topic -> " + r.statusCode());

        step("create subscription with options");
        r = mgmt("PUT", TOPIC + "/Subscriptions/" + SUBSCRIPTION, entry("SubscriptionDescription",
            "<LockDuration>PT45S</LockDuration><MaxDeliveryCount>4</MaxDeliveryCount>"));
        check(created(r), "create subscription -> " + r.statusCode());

        step("get subscription and confirm round-trip");
        r = mgmt("GET", TOPIC + "/Subscriptions/" + SUBSCRIPTION, null);
        check(r.statusCode() == 200, "get subscription -> " + r.statusCode());
        check(r.body().contains("<LockDuration>PT45S</LockDuration>"), "subscription echoes LockDuration");

        step("create SQL rule");
        r = mgmt("PUT", TOPIC + "/Subscriptions/" + SUBSCRIPTION + "/Rules/" + RULE, entry("RuleDescription",
            "<Filter i:type=\"SqlFilter\"><SqlExpression>priority = 'high'</SqlExpression></Filter>"
                + "<Action i:type=\"EmptyRuleAction\" /><Name>" + RULE + "</Name>"));
        check(created(r), "create rule -> " + r.statusCode());

        step("list entities");
        r = mgmt("GET", "$Resources/queues", null);
        check(r.statusCode() == 200 && r.body().contains(QUEUE), "list queues includes the queue");
        r = mgmt("GET", "$Resources/topics", null);
        check(r.statusCode() == 200 && r.body().contains(TOPIC), "list topics includes the topic");
        r = mgmt("GET", TOPIC + "/Subscriptions", null);
        check(r.statusCode() == 200 && r.body().contains(SUBSCRIPTION), "list subscriptions includes the subscription");
        r = mgmt("GET", TOPIC + "/Subscriptions/" + SUBSCRIPTION + "/Rules", null);
        check(r.statusCode() == 200 && r.body().contains(RULE) && r.body().contains("$Default"),
            "list rules includes $Default + our rule");

        step("usage: send and receive on the admin-created queue (data plane on 5672)");
        try (ServiceBusSenderClient sender = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING).sender().queueName(QUEUE).buildClient()) {
            sender.sendMessage(new ServiceBusMessage("hello-admin").setSubject("AdminCreated"));
        }
        try (ServiceBusReceiverClient receiver = new ServiceBusClientBuilder()
                .connectionString(CONNECTION_STRING).receiver()
                .receiveMode(ServiceBusReceiveMode.PEEK_LOCK).queueName(QUEUE).buildClient()) {
            List<ServiceBusReceivedMessage> msgs =
                receiver.receiveMessages(1, Duration.ofSeconds(10)).stream().toList();
            check(msgs.size() == 1, "received one message (got " + msgs.size() + ")");
            check(msgs.get(0).getBody().toString().equals("hello-admin"), "message body round-trips");
            check("AdminCreated".equals(msgs.get(0).getSubject()),
                "subject round-trips (" + msgs.get(0).getSubject() + ")");
            receiver.complete(msgs.get(0));
        }

        step("delete rule, subscription, topic, queue");
        check(created(mgmt("DELETE", TOPIC + "/Subscriptions/" + SUBSCRIPTION + "/Rules/" + RULE, null)),
            "delete rule");
        check(created(mgmt("DELETE", TOPIC + "/Subscriptions/" + SUBSCRIPTION, null)), "delete subscription");
        check(created(mgmt("DELETE", TOPIC, null)), "delete topic");
        check(created(mgmt("DELETE", QUEUE, null)), "delete queue");
        check(mgmt("GET", QUEUE, null).statusCode() == 404, "deleted queue is gone (404)");

        System.out.println("\nJava admin test passed");
    }
}
