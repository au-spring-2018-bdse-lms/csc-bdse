package ru.csc.bdse.kv;

import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Constants;
import ru.csc.bdse.util.Env;
import ru.csc.bdse.util.Random;

import java.io.File;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

import static java.time.temporal.ChronoUnit.SECONDS;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */
public class KeyValueApiHttpClientTest2 {
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

    private KeyValueApi api = newKeyValueApi();

    private KeyValueApi newKeyValueApi() {
        final String baseUrl = "http://localhost:" + node.getMappedPort(8080);
        return new KeyValueApiHttpClient(baseUrl);
    }

    @Test
    public void concurrentPuts() throws InterruptedException {
        String key = Random.nextKey();
        byte[] value = Random.nextValue();
        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 100; i++) {
            executor.submit(() -> {
                api.put(key, value);
                byte[] newValue = api.get(key).orElse(Constants.EMPTY_BYTE_ARRAY);
                assertArrayEquals(value, newValue);
            });
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        byte[] newValue = api.get(key).orElse(Constants.EMPTY_BYTE_ARRAY);
        assertArrayEquals(value, newValue);
    }

    @Test
    public void concurrentDeleteAndKeys() throws InterruptedException {
        Map<String, byte[]> data = new HashMap<>();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String key = Random.nextKey();
            byte[] value = Random.nextValue();
            keys.add(key);
            data.put(key, value);
            api.put(key, value);
            api.put(key + "a", value);
        }

        ExecutorService executor = Executors.newFixedThreadPool(5);
        for (int i = 0; i < 100; i++) {
            int finalI = i;
            executor.submit(() -> {
                String key = keys.get(finalI);
                byte[] value = data.get(key);
                byte[] newValue = api.get(key).orElse(Constants.EMPTY_BYTE_ARRAY);

                assertArrayEquals(value, newValue);
                Set<String> keysWithPrefix = api.getKeys(key);
                assertTrue(keysWithPrefix.contains(key));
                assertTrue(keysWithPrefix.contains(key + "a"));

                api.delete(key);
                api.delete(key + "a");

                Set<String> keysWithPrefixNew = api.getKeys(key);
                assertFalse(keysWithPrefixNew.contains(key));
                assertFalse(keysWithPrefixNew.contains(key + "a"));
            });
        }
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        Set<String> currentKeys = api.getKeys("");
        for (int i = 0; i < 100; i++) {
            String key = keys.get(i);
            assertFalse(currentKeys.contains(key));
            assertFalse(currentKeys.contains(key + "a"));
        }
    }

    @Test
    public void actionUpDown() {
        //TODO test up/down actions
    }

    @Test
    public void putWithStoppedNode() {
        //TODO test put if node/container was stopped
    }

    @Test
    public void getWithStoppedNode() {
        //TODO test get if node/container was stopped
    }

    @Test
    public void getKeysByPrefixWithStoppedNode() {
        //TODO test getKeysByPrefix if node/container was stopped
    }

    @Test
    public void deleteByTombstone() {
        // TODO use tombstones to mark as deleted (optional)
    }

    @Test
    public void loadMillionKeys() {
        //TODO load too many data (optional)
    }
}


