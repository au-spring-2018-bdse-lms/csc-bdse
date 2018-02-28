package ru.csc.bdse.kv.redis;

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
    public void genTest() {
        api.buildRedis();
    }
}