package ru.csc.bdse.kv;

import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;
import ru.csc.bdse.kv.redis.RedisKeyValueApi;

import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

public class RedisKeyValueApiTest extends AbstractKeyValueApiTest {
    private static final int REDIS_PORT = 6379;

    @ClassRule
    public static final GenericContainer node = new GenericContainer("redis:3.2.11")
            .withExposedPorts(REDIS_PORT)
            .withStartupTimeout(Duration.of(30, SECONDS));

    @Override
    protected KeyValueApi newKeyValueApi() {
        final String baseUrl = "redis://localhost:" + node.getMappedPort(REDIS_PORT);
        return new RedisKeyValueApi("redis-node", baseUrl);
    }
}
