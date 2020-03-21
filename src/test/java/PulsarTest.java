import com.github.dockerjava.api.model.ContainerNetwork;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.*;
import org.testcontainers.containers.wait.strategy.Wait;
import org.apache.pulsar.client.admin.PulsarAdmin;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class PulsarTest {
    private static final String PULSAR = "pulsar://pulsar:6650";
    private static final String PULSAR_SERVICE = "pulsar_1";
    private static final int PULSAR_ADMIN_PORT = 8080;
    private static final int PULSAR_DATA_PORT = 6650;
    private static final String PULSAR_ADMIN_PROTOCOL = "http";

    @Test
    public void mainTest() throws IOException, PulsarAdminException, InterruptedException {

        System.out.println("Starting Pulsar...");
        DockerComposeContainer pulsarEnv = startPulsarEnv();
        System.out.println("Pulsar has started.");

        final String pulsarNetworkName = getPulsarNetworkName(pulsarEnv);
        System.out.println("Pulsar network is '" + pulsarNetworkName + "'");

        System.out.println("Configuring Pulsar...");
        configurePulsar(pulsarEnv);
        System.out.println("Finished configuring Pulsar.");

        System.out.println("Starting Main Consumer...");
        DockerComposeContainer consumerEnv = startConsumer(pulsarNetworkName);
        System.out.println("Main Consumer is running.");

        System.out.println("Starting Producers...");
        DockerComposeContainer producersEnv = startProducers(pulsarNetworkName);
        System.out.println("Producers are running and sending messages.");

        System.out.println("Checking that Main Consumer is receiving messages...");
        //Wait a little, so the consumer has a chance to start receiving the messages
        Thread.sleep(2000);

        ContainerState consumerCS = (ContainerState)consumerEnv.getContainerByServiceName("main-consumer_1").get();
        assertTrue(consumerCS.getLogs().contains("Got message from customer: customer1"));
        assertTrue(consumerCS.getLogs().contains("Got message from customer: customer2"));
        System.out.println("Main Consumer is receiving messages from all producers.");

        System.out.println("Stopping all containers...");
        // Order is important, Pulsar can be only be stopped last as others use its network
        producersEnv.stop();
        consumerEnv.stop();
        pulsarEnv.stop();
        System.out.println("All containers have stopped.");

        System.out.println("Test finished successfully!");
    }

    private PulsarAdmin getPulsarAdmin(DockerComposeContainer pulsarEnv) throws PulsarClientException {
        return PulsarAdmin.builder()
                .serviceHttpUrl(String.format("%s://%s:%s",
                        PULSAR_ADMIN_PROTOCOL,
                        pulsarEnv.getServiceHost(PULSAR_SERVICE, PULSAR_ADMIN_PORT),
                        pulsarEnv.getServicePort(PULSAR_SERVICE, PULSAR_ADMIN_PORT)))
                .build();
    }

    private TenantInfo getGeneralTenantInfo(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        return new TenantInfo(new HashSet<>(Collections.emptyList()), new HashSet<>(pulsarAdmin.clusters().getClusters()));
    }

    private void pulsarOneTimeSetup(PulsarAdmin pulsarAdmin) throws PulsarAdminException {
        pulsarAdmin.tenants().createTenant("internal", getGeneralTenantInfo(pulsarAdmin));
        pulsarAdmin.namespaces().createNamespace("internal/inbound");
        pulsarAdmin.topics().createNonPartitionedTopic("internal/inbound/corona");
    }

    private void pulsarCustomerOnBoarding(PulsarAdmin pulsarAdmin, String customer) throws PulsarAdminException {
        pulsarAdmin.tenants().createTenant(customer, getGeneralTenantInfo(pulsarAdmin));
        pulsarAdmin.namespaces().createNamespace(customer + "/outbound");
        pulsarAdmin.topics().createNonPartitionedTopic(customer + "/outbound/corona");

        FunctionConfig functionConfig = FunctionConfig.builder()
                .name("in_router")
                .tenant(customer)
                .namespace("outbound")
                .inputs(Collections.singletonList("persistent://" + customer + "/outbound/corona"))
                .className("in_router.RoutingFunction")
                .py("in_router.py").build();
        pulsarAdmin.functions().createFunction(functionConfig, "pulsar-function/in_router.py");
    }

    private void configurePulsar(DockerComposeContainer pulsarEnv) throws PulsarClientException, PulsarAdminException {
        PulsarAdmin pulsarAdmin = getPulsarAdmin(pulsarEnv);

        pulsarOneTimeSetup(pulsarAdmin);

        System.out.println("On-boarding customer1 to Pulsar...");
        pulsarCustomerOnBoarding(pulsarAdmin, "customer1");

        System.out.println("On-boarding customer2 to Pulsar...");
        pulsarCustomerOnBoarding(pulsarAdmin, "customer2");

        pulsarAdmin.close();
    }

    private String getPulsarNetworkName(DockerComposeContainer pulsarEnv) {
        ContainerState cs = (ContainerState)pulsarEnv.getContainerByServiceName(PULSAR_SERVICE).get();
        Map<String, ContainerNetwork> cns = cs.getCurrentContainerInfo().getNetworkSettings().getNetworks();
        // We know that the compose file defines exactly one network, so get its name
        return cns.keySet().iterator().next();
    }

    private DockerComposeContainer startPulsarEnv()  {
        DockerComposeContainer pulsarEnv = new DockerComposeContainer("pulsar", new File("docker-compose/pulsar.docker-compose.yml"))
                .withExposedService(PULSAR_SERVICE, PULSAR_ADMIN_PORT, Wait.forHttp("/metrics").forStatusCode(200).forPort(PULSAR_ADMIN_PORT))
                .withExposedService(PULSAR_SERVICE, PULSAR_DATA_PORT, Wait.forListeningPort());

        pulsarEnv.start();
        return pulsarEnv;
    }

    private DockerComposeContainer startConsumer(String pulsarNetworkName) {
        DockerComposeContainer consumerEnv = new DockerComposeContainer(new File("docker-compose/consumer.docker-compose.yml"))
                .withEnv("PULSAR_NETWORK", pulsarNetworkName)
                .withEnv("PULSAR", PULSAR)
                .waitingFor("main-consumer_1", Wait.forLogMessage(".*Consumer started.*", 1));

        consumerEnv.start();
        return consumerEnv;
    }

    private DockerComposeContainer startProducers(String pulsarNetworkName) {
        DockerComposeContainer producersEnv = new DockerComposeContainer(new File("docker-compose/producers.docker-compose.yml"))
                .withEnv("PULSAR_NETWORK", pulsarNetworkName)
                .withEnv("PULSAR", PULSAR)
                .waitingFor("customer1-producer_1", Wait.forLogMessage(".*Sending message to topic:.*", 1))
                .waitingFor("customer2-producer_1", Wait.forLogMessage(".*Sending message to topic:.*", 1));

        producersEnv.start();
        return producersEnv;
    }
}
