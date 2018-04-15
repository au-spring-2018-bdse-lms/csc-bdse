package ru.csc.bdse.kv;

import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Env;
import ru.csc.bdse.util.Random;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class ContaineredReplicasTest extends AbstractKeyValueApiTest {
    private static final int REDIS_PORT = 6379;
    private static final Network network = Network.newNetwork();
    private static GenericContainer[] redisNodes, nodes;
    private static KeyValueApi[] innerNodes;

    @Parameterized.Parameters(name = "RF={0}, WCL={1}, RCL={2}git st")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {1, 1, 1},
                {3, 3, 1}, {3, 1, 3},
                {3, 2, 2}, {5, 3, 3}
        });
    }

    private static int replicas;
    private static int wcl;
    private static int rcl;

    public ContaineredReplicasTest(int replicas, int wcl, int rcl) {
        if (ContaineredReplicasTest.replicas == replicas && ContaineredReplicasTest.wcl == wcl &&
                ContaineredReplicasTest.rcl == rcl) {
            return;
        }
        stopNodes();
        ContaineredReplicasTest.replicas = replicas;
        ContaineredReplicasTest.wcl = wcl;
        ContaineredReplicasTest.rcl = rcl;

        redisNodes = new GenericContainer[replicas];
        for (int i = 0; i < redisNodes.length; i++) {
            redisNodes[i] = new GenericContainer("redis:3.2.11")
                    .withExposedPorts(REDIS_PORT)
                    .withNetwork(network)
                    .withNetworkAliases("redis-node" + i)
                    .withStartupTimeout(Duration.of(30, SECONDS));
            redisNodes[i].start();
        }

        nodes = new GenericContainer[replicas];
        innerNodes = new KeyValueApi[replicas];
        for (int i = 0; i < replicas; i++) {
            List<String> peers = new ArrayList<>();
            for (int j = 0; j < replicas; j++)
                if (i != j) {
                    peers.add("http://node" + j + ":8080/inner");
                }
            nodes[i] = createNode("node" + i, String.join(",", peers));
            nodes[i].start();
            innerNodes[i] = new KeyValueApiHttpClient("http://localhost" + ":" + nodes[i].getMappedPort(8080) + "/inner");
        }
    }

    @AfterClass
    public static void tearDown() {
        stopNodes();
    }

    private static void stopNodes() {
        if (nodes != null) {
            for (GenericContainer node : nodes) {
                if (node != null) {
                    node.stop();
                }
            }
        }
        if (redisNodes != null) {
            for (GenericContainer node : redisNodes) {
                if (node != null) {
                    node.stop();
                }
            }
        }
    }

    protected GenericContainer createNode(String nodeName, String replicasUrl) {
        return new GenericContainer(
                new ImageFromDockerfile()
                        .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                                ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                        .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
                .withEnv(Env.KVNODE_NAME, nodeName)
                .withEnv(Env.KVNODE_REDIS_URI, "redis://redis-" + nodeName + ":" + REDIS_PORT)
                .withEnv(Env.KVNODE_REPLICAS_URL, replicasUrl)
                .withEnv(Env.KVNODE_REPLICA_TIMEOUT_MS, "1000")
                .withEnv(Env.KVNODE_WCL, Integer.toString(wcl))
                .withEnv(Env.KVNODE_RCL, Integer.toString(rcl))
                .withExposedPorts(8080)
                .withNetwork(network)
                .withNetworkAliases(nodeName)
                .withStartupTimeout(Duration.of(30, SECONDS));
    }

    @Override
    protected KeyValueApi newKeyValueApi() {
        return new FailoverKeyValueApi(
                Arrays.stream(nodes).map(
                        node -> new KeyValueApiHttpClient("http://localhost:" + node.getMappedPort(8080))
                ).collect(Collectors.toList()));
    }


    @Override
    protected int numberOfNodes() {
        return replicas;
    }

    @Test
    public void writeKillRead() {
        String key = Random.nextKey();
        byte[] value = Random.nextValue();

        api.put(key, value);

        for (int i = 0; i < replicas; i++) {
            System.out.println("Pausing node " + i);
            innerNodes[i].action("node" + i, NodeAction.DOWN);

            boolean succeeded = false;
            try {
                assertArrayEquals(value, api.get(key).get());
                succeeded = true;
            } catch (NodeUnavailableException ignored) {
            } finally {
                innerNodes[i].action("node" + i, NodeAction.UP);
            }

            assertEquals(rcl < replicas, succeeded);
        }
    }

    @Test
    public void writeKillOverwriteRestoreRead() {
        String key = Random.nextKey();

        byte[] oldValue = Random.nextValue();
        api.put(key, oldValue);
        for (int i = 0; i < replicas; i++) {
            byte[] newValue = Random.nextValue();
            System.out.println("Pausing node " + i);
            innerNodes[i].action("node" + i, NodeAction.DOWN);

            boolean succeeded = false;
            try {
                api.put(key, newValue);
                succeeded = true;
            } catch (NodeUnavailableException ignored) {
            } finally {
                innerNodes[i].action("node" + i, NodeAction.UP);
            }

            if (wcl < replicas) {
                assertTrue(succeeded);
                assertArrayEquals(newValue, api.get(key).get());
            } else {
                assertFalse(succeeded);
                assertThat(api.get(key).get(),
                        Matchers.anyOf(
                                Matchers.equalTo(oldValue),
                                Matchers.equalTo(newValue)
                        ));
            }
        }
    }
}
