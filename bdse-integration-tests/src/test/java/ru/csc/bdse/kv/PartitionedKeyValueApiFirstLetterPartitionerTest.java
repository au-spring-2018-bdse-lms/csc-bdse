package ru.csc.bdse.kv;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.kv.partitioned.ShardsConfiguration;
import ru.csc.bdse.kv.partitioned.ShardsCoordinator;
import ru.csc.bdse.partitioning.FirstLetterPartitioner;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.time.temporal.ChronoUnit.SECONDS;

public class PartitionedKeyValueApiFirstLetterPartitionerTest extends AbstractPartitionedKeyValueApiHttpClientTest {
    private static final int REDIS_PORT = 6379;
    private static final Network network = Network.newNetwork();
    private static GenericContainer[] redisNodes, nodes;
    private static KeyValueApi[] innerNodes;
    private static Set<String> keys = Stream.generate(() -> RandomStringUtils.randomAlphanumeric(10)).limit(1000)
                .collect(Collectors.toSet());

    @BeforeClass
    public static void setUp() {
        redisNodes = new GenericContainer[3];
        for (int i = 0; i < redisNodes.length; i++) {
            redisNodes[i] = new GenericContainer("redis:3.2.11")
                    .withExposedPorts(REDIS_PORT)
                    .withNetwork(network)
                    .withNetworkAliases("redis-node" + i)
                    .withStartupTimeout(Duration.of(30, SECONDS));
            redisNodes[i].start();
        }

        nodes = new GenericContainer[3];
        innerNodes = new KeyValueApi[3];
        for (int i = 0; i < 3; i++) {
            nodes[i] = createNode("node" + i);
            nodes[i].start();
            innerNodes[i] = new KeyValueApiHttpClient("http://localhost:" + nodes[i].getMappedPort(8080));
        }
    }

    @AfterClass
    public static void tearDown() {
        for (int i = 0; i < 3; i++) {
            nodes[i].stop();
            redisNodes[i].stop();
        }
    }

    private static GenericContainer createNode(String nodeName) {
        return new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                                ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                        .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
                .withEnv(Env.KVNODE_NAME, nodeName)
                .withEnv(Env.KVNODE_REDIS_URI, "redis://redis-" + nodeName + ":" + REDIS_PORT)
                .withExposedPorts(8080)
                .withNetwork(network)
                .withNetworkAliases(nodeName)
                .withStartupTimeout(Duration.of(30, SECONDS));
    }

    @Override
    protected KeyValueApi newCluster1() {
        Map<String, KeyValueApi> shards = new HashMap<>();
        shards.put("node0", innerNodes[0]);
        shards.put("node1", innerNodes[1]);
        shards.put("node2", innerNodes[2]);
        return new ShardsCoordinator(new ShardsConfiguration(
                shards,
                3000,
                new FirstLetterPartitioner(shards.keySet())
        ));
    }

    @Override
    protected KeyValueApi newCluster2() {
        Map<String, KeyValueApi> shards = new HashMap<>();
        shards.put("node0", innerNodes[0]);
        shards.put("node2", innerNodes[2]);
        return new ShardsCoordinator(new ShardsConfiguration(
                shards,
                3000,
                new FirstLetterPartitioner(shards.keySet())
        ));
    }

    @Override
    protected Set<String> keys() {
        return keys;
    }

    @Override
    protected float expectedKeysLossProportion() {
        return (float) innerNodes[1].getKeys("").size() / keys.size();
    }

    @Override
    protected float expectedUndeletedKeysProportion() {
        return (float) innerNodes[1].getKeys("").size() / keys.size();
    }
}
