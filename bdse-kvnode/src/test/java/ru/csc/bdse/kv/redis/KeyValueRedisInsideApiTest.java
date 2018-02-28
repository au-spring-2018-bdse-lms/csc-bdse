package ru.csc.bdse.kv.redis;

import com.spotify.docker.client.exceptions.DockerException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import ru.csc.bdse.kv.NodeStatus;

import static org.junit.Assert.*;

public class KeyValueRedisInsideApiTest {
    @Rule
    public ExpectedException thrown = ExpectedException.none();
    private KeyValueRedisInsideApi api;

    @Before
    public void setUp() throws Exception {
        api = new KeyValueRedisInsideApi();
    }
    @Test
    public void startStopExistingTest() throws Exception {
        assertEquals(api.getStatus() , NodeStatus.DOWN);
        api.upRedis();
        assertEquals(api.getStatus() , NodeStatus.UP);
        api.stopRedis();
        assertEquals(api.getStatus() , NodeStatus.DOWN);
    }

    @Test
    public void genTest() throws DockerException, InterruptedException {
        try {
            api.getStatus();
        } catch (Exception e) {
            api.buildRedis();
        }
    }

    @Test
    public void redisclient() throws DockerException, InterruptedException {
        if (api.getStatus() == NodeStatus.DOWN) {
            api.upRedis();
        }
        api.tryConnect();
    }
}