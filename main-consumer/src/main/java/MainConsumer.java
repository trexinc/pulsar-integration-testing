import org.apache.pulsar.client.api.*;

import java.nio.charset.StandardCharsets;

public class MainConsumer {
    private static Consumer<byte[]> consumer;

    public static void main(String[] args) throws PulsarClientException {
        consumer = PulsarClient.builder()
                .serviceUrl(System.getenv("PULSAR"))
                .build()
                .newConsumer()
                .topic(System.getenv("TOPIC"))
                .subscriptionName("subscription")
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribe();

        consume();

        consumer.close();
    }

    private static void consume() throws PulsarClientException {
        while (true) {
            Message<byte[]> message = consumer.receive();
            System.out.println("Got message from customer: " + message.getProperty("customer")
                    + " on topic " + message.getTopicName()
                    + " message: " + new String(message.getData(), StandardCharsets.UTF_8));
            consumer.acknowledge(message);
        }
    }
}
