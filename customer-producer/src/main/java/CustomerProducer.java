import com.google.gson.Gson;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import java.util.Random;

public class CustomerProducer {
    private static Producer<byte[]> producer;
    private static Gson gson = new Gson();
    private static Random random = new Random();
    private static String TOPIC = System.getenv("TOPIC");

    public static void main(String[] args) throws PulsarClientException, InterruptedException {
        producer = PulsarClient.builder()
                .serviceUrl(System.getenv("PULSAR"))
                .build()
                .newProducer()
                .topic(TOPIC)
                .create();

        produce();

        producer.close();
    }

    private static void produce() throws PulsarClientException, InterruptedException {
        while (true) {
            String message = gson.toJson(new CustomerMessage(random.nextLong(), "COVID-19" ));
            System.out.println("Sending message to topic: " + TOPIC + " message: " + message);
            producer.send(message.getBytes());
            Thread.sleep(3000);
        }
    }
}
