package ru.csc.bdse.kv;

import org.assertj.core.api.SoftAssertions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.time.temporal.ChronoUnit.SECONDS;

public class ContaineredReplicasTest extends AbstractKeyValueApiTest {
    private static final int REDIS_PORT = 6379;
    private static final Network network = Network.newNetwork();
    private static GenericContainer[] redisNodes, nodes;

    private static final int REPLICAS = 3;
    private static final int WCL = 2;
    private static final int RCL = 2;

    @BeforeClass
    public static void setUp() {
        redisNodes = new GenericContainer[REPLICAS];
        for (int i = 0; i < redisNodes.length; i++) {
            redisNodes[i] = new GenericContainer("redis:3.2.11")
                    .withExposedPorts(REDIS_PORT)
                    .withNetwork(network)
                    .withNetworkAliases("redis-node" + i)
                    .withStartupTimeout(Duration.of(30, SECONDS));
        }
        nodes = new GenericContainer[REPLICAS];
        for (int i = 0; i < REPLICAS; i++) {
            List<String> peers = new ArrayList<>();
            for (int j = 0; j < REPLICAS; j++)
                if (i != j) {
                    peers.add("http://node" + j + ":8080/inner");
                }
            nodes[i] = createNode("node" + i, String.join(",", peers));
        }
        for (GenericContainer node : redisNodes) {
            node.start();
        }
        for (GenericContainer node : nodes) {
            node.start();
        }
    }

    @AfterClass
    public static void tearDown() {
        if (nodes != null) {
            for (GenericContainer node : nodes) {
                if (node != null) {
                    node.stop();
                }
            }
        }
        if (nodes != null) {
            for (GenericContainer node : redisNodes) {
                if (node != null) {
                    node.stop();
                }
            }
        }
    }

    protected static GenericContainer createNode(String nodeName, String replicasUrl) {
        return new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                                ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                        .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
                .withEnv(Env.KVNODE_NAME, nodeName)
                .withEnv(Env.KVNODE_REDIS_URI, "redis://redis-" + nodeName + ":" + REDIS_PORT)
                .withEnv(Env.KVNODE_REPLICAS_URL, replicasUrl)
                .withEnv(Env.KVNODE_REPLICA_TIMEOUT_MS, "1000")
                .withEnv(Env.KVNODE_WCL, Integer.toString(WCL))
                .withEnv(Env.KVNODE_RCL, Integer.toString(RCL))
                .withExposedPorts(8080)
                .withNetwork(network)
                .withNetworkAliases(nodeName)
                .withStartupTimeout(Duration.of(30, SECONDS));
    }

    @Override
    protected KeyValueApi newKeyValueApi() {
        final String baseUrl = "http://localhost:" + nodes[0].getMappedPort(8080);
        return new KeyValueApiHttpClient(baseUrl);
    }


    @Override
    protected int numberOfNodes() {
        return REPLICAS;
    }
}
