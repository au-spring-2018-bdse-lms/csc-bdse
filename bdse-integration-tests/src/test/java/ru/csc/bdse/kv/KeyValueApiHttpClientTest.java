package ru.csc.bdse.kv;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

/**
 * @author semkagtn
 */
public class KeyValueApiHttpClientTest extends AbstractKeyValueApiTest {
    private static final int REDIS_PORT = 6379;
    private static final Network network = Network.newNetwork();
    private static GenericContainer redisNode;
    private static GenericContainer node;

    @BeforeClass
    public static void setUp() {
        redisNode = new GenericContainer("redis:3.2.11")
                .withExposedPorts(REDIS_PORT)
                .withNetwork(network)
                .withNetworkAliases("redis")
                .withStartupTimeout(Duration.of(30, SECONDS));
        redisNode.start();
        node = new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                                ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                        .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
                .withEnv(Env.KVNODE_NAME, "node-0")
                .withEnv(Env.KVNODE_REDIS_URI, "redis://redis:" + REDIS_PORT)
                .withExposedPorts(8080)
                .withNetwork(network)
                .withStartupTimeout(Duration.of(30, SECONDS));
        node.start();
    }

    @AfterClass
    public static void tearDown() {
        node.stop();
        redisNode.stop();
    }

    @Override
    protected KeyValueApi newKeyValueApi() {
        final String baseUrl = "http://localhost:" + node.getMappedPort(8080);
        return new KeyValueApiHttpClient(baseUrl);
    }
}
