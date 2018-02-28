package ru.csc.bdse.kv.redis;

import com.spotify.docker.client.exceptions.DockerException;
import org.junit.*;
import org.junit.rules.ExpectedException;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeStatus;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;

import static org.junit.Assert.*;

public class KeyValueRedisInsideApiTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private KeyValueRedisInsideApi api;

    @Before
    public void setUp() throws Exception {
        api = new KeyValueRedisInsideApi("test node");
    }
    @Test
    @Ignore
    public void startStopExistingTest() throws Exception {
        assertEquals(api.getStatus() , NodeStatus.DOWN);
        api.upRedis();
        assertEquals(api.getStatus() , NodeStatus.UP);
        api.stopRedis();
        assertEquals(api.getStatus() , NodeStatus.DOWN);
    }

    @Test
    @Ignore
    public void genTest() throws DockerException, InterruptedException {
        try {
            api.getStatus();
        } catch (Exception e) {
            api.buildRedis();
        }
    }

    @Test
    @Ignore
    public void redisclient() throws DockerException, InterruptedException {
        if (api.getStatus() == NodeStatus.DOWN) {
            api.upRedis();
        }
//        api.tryConnect();
    }

    @Ignore
    @Test
    public void testSimple() {
        api.action("test node", NodeAction.UP);
        String key = "a";
        byte[] value = "b".getBytes();
        api.put(key, value);
        Assert.assertEquals(api.getInfo().iterator().next().getName(), "test node");
        Assert.assertEquals(api.getInfo().iterator().next().getStatus(), NodeStatus.UP);
        api.action("test node", NodeAction.DOWN);
        Assert.assertEquals(api.getInfo().iterator().next().getStatus(), NodeStatus.DOWN);
        try {
            Assert.assertArrayEquals(value, api.get(key).get());
        } catch (NoSuchElementException e) {
            api.action("", NodeAction.UP);
            Assert.assertEquals(api.getInfo().iterator().next().getStatus(), NodeStatus.UP);
            Assert.assertArrayEquals(value, api.get(key).get());
        }
    }

}