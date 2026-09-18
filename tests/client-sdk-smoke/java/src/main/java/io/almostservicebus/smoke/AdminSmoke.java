package io.almostservicebus.smoke;

import com.azure.core.exception.ResourceNotFoundException;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusMessage;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverClient;
import com.azure.messaging.servicebus.ServiceBusSenderClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClient;
import com.azure.messaging.servicebus.administration.ServiceBusAdministrationClientBuilder;
import com.azure.messaging.servicebus.administration.models.CreateQueueOptions;
import com.azure.messaging.servicebus.administration.models.CreateRuleOptions;
import com.azure.messaging.servicebus.administration.models.CreateSubscriptionOptions;
import com.azure.messaging.servicebus.administration.models.QueueProperties;
import com.azure.messaging.servicebus.administration.models.SqlRuleFilter;
import com.azure.messaging.servicebus.administration.models.SubscriptionProperties;
import com.azure.messaging.servicebus.models.ServiceBusReceiveMode;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Admin-interface test: exercises the official Java Azure Service Bus SDK's
 * {@code ServiceBusAdministrationClient} against a running AlmostServiceBus emulator, covering
 * queue / topic / subscription / rule create, get, list, update and delete, plus a data-plane
 * round-trip on an admin-created queue to prove the entity is usable.
 *
 * <p>The Java {@code ServiceBusAdministrationClientBuilder} strips the port from the endpoint and
 * always speaks HTTPS to the namespace host on <strong>port 443</strong> (both
 * {@code connectionString(...)} and {@code endpoint(...)} reduce to {@code URI.getHost()}). So run
 * the emulator's HTTPS admin endpoint on 443 and point this test at a <em>portless</em> connection
 * string:
 *
 * <pre>
 * almost-servicebus --AdminTlsEnabled true --AdminTlsPort 443   # 443 is privileged
 * ASB_ADMIN_CONNECTION_STRING=Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=emulator;UseDevelopmentEmulator=true
 * </pre>
 *
 * <p>Java trusts the emulator CA through a PKCS12 truststore passed as system properties (see
 * ../../../certs/README.md):
 *
 * <pre>
 * java -Djavax.net.ssl.trustStore=&lt;certdir&gt;/emulator-truststore.p12 \
 *      -Djavax.net.ssl.trustStoreType=PKCS12 \
 *      -Djavax.net.ssl.trustStorePassword=changeit ...
 * </pre>
 *
 * <p>The data-plane round-trip uses the SDK against the plain-AMQP endpoint on 5672. Exits
 * non-zero on the first failed check.
 */
public final class AdminSmoke {

    private static final String ADMIN_CONNECTION_STRING = System.getenv().getOrDefault(
        "ASB_ADMIN_CONNECTION_STRING",
        "Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;"
            + "SharedAccessKey=emulator;UseDevelopmentEmulator=true");

    private static final String DATA_CONNECTION_STRING = System.getenv().getOrDefault(
        "ASB_CONNECTION_STRING",
        "Endpoint=sb://localhost:5672;SharedAccessKeyName=RootManageSharedAccessKey;"
            + "SharedAccessKey=emulator;UseDevelopmentEmulator=true");

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

    public static void main(String[] args) {
        ServiceBusAdministrationClient admin = new ServiceBusAdministrationClientBuilder()
            .connectionString(ADMIN_CONNECTION_STRING)
            .buildClient();

        step("create queue with options (SDK admin client over HTTPS)");
        QueueProperties queue = admin.createQueue(QUEUE, new CreateQueueOptions()
            .setLockDuration(Duration.parse("PT1M"))
            .setMaxDeliveryCount(5)
            .setMaxSizeInMegabytes(2048)
            .setDeadLetteringOnMessageExpiration(true));
        check(QUEUE.equals(queue.getName()), "queue created (" + queue.getName() + ")");
        check(queue.getMaxDeliveryCount() == 5, "maxDeliveryCount set (" + queue.getMaxDeliveryCount() + ")");

        step("get queue and confirm options round-trip");
        QueueProperties gotQueue = admin.getQueue(QUEUE);
        check(gotQueue.getMaxDeliveryCount() == 5,
            "maxDeliveryCount round-trips (" + gotQueue.getMaxDeliveryCount() + ")");
        check(gotQueue.getMaxSizeInMegabytes() == 2048,
            "maxSizeInMegabytes round-trips (" + gotQueue.getMaxSizeInMegabytes() + ")");
        check(gotQueue.isDeadLetteringOnMessageExpiration(), "deadLetteringOnMessageExpiration round-trips");
        check(Duration.parse("PT1M").equals(gotQueue.getLockDuration()),
            "lockDuration round-trips (" + gotQueue.getLockDuration() + ")");

        step("update queue");
        gotQueue.setMaxDeliveryCount(8);
        admin.updateQueue(gotQueue);
        check(admin.getQueue(QUEUE).getMaxDeliveryCount() == 8, "updated maxDeliveryCount persisted");

        step("create topic and confirm");
        check(TOPIC.equals(admin.createTopic(TOPIC).getName()), "topic created (" + TOPIC + ")");

        step("create subscription with options");
        SubscriptionProperties subscription = admin.createSubscription(TOPIC, SUBSCRIPTION,
            new CreateSubscriptionOptions()
                .setLockDuration(Duration.parse("PT45S"))
                .setMaxDeliveryCount(4)
                .setDeadLetteringOnMessageExpiration(true));
        check(SUBSCRIPTION.equals(subscription.getSubscriptionName()),
            "subscription created (" + subscription.getSubscriptionName() + ")");
        check(subscription.getMaxDeliveryCount() == 4,
            "subscription maxDeliveryCount (" + subscription.getMaxDeliveryCount() + ")");

        step("get subscription and confirm round-trip");
        SubscriptionProperties gotSub = admin.getSubscription(TOPIC, SUBSCRIPTION);
        check(Duration.parse("PT45S").equals(gotSub.getLockDuration()),
            "subscription lockDuration round-trips (" + gotSub.getLockDuration() + ")");

        step("create SQL rule");
        check(RULE.equals(admin.createRule(TOPIC, RULE, SUBSCRIPTION,
            new CreateRuleOptions(new SqlRuleFilter("priority = 'high'"))).getName()),
            "rule created (" + RULE + ")");

        step("list entities");
        check(admin.listQueues().stream().count() >= 1, "listQueues returns the queue");
        check(admin.listTopics().stream().count() >= 1, "listTopics returns the topic");
        check(admin.listSubscriptions(TOPIC).stream().count() == 1, "listSubscriptions returns one subscription");
        // A subscription always keeps its implicit $Default rule alongside the one we added.
        check(admin.listRules(TOPIC, SUBSCRIPTION).stream().count() == 2, "listRules returns $Default + our rule");

        step("usage: send and receive on the admin-created queue (data plane on 5672)");
        try (ServiceBusSenderClient sender = new ServiceBusClientBuilder()
                .connectionString(DATA_CONNECTION_STRING).sender().queueName(QUEUE).buildClient()) {
            sender.sendMessage(new ServiceBusMessage("hello-admin").setSubject("AdminCreated"));
        }
        try (ServiceBusReceiverClient receiver = new ServiceBusClientBuilder()
                .connectionString(DATA_CONNECTION_STRING).receiver()
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
        admin.deleteRule(TOPIC, SUBSCRIPTION, RULE);
        admin.deleteSubscription(TOPIC, SUBSCRIPTION);
        admin.deleteTopic(TOPIC);
        admin.deleteQueue(QUEUE);
        boolean gone = false;
        try {
            admin.getQueue(QUEUE);
        } catch (ResourceNotFoundException e) {
            gone = true;
        }
        check(gone, "deleted queue is gone (404)");

        System.out.println("\nJava admin test passed");
    }
}
