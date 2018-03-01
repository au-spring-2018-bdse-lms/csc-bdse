package ru.csc.bdse.kv.redis;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisException;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import ru.csc.bdse.kv.*;
import ru.csc.bdse.util.Require;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class RedisKeyValueApi implements KeyValueApi {
    private final String name;
    private final String redisUri;

    private static class RedisConnection implements AutoCloseable {
        private final RedisClient client;
        private final StatefulRedisConnection<String, byte[]> connection;
        private final RedisCommands<String, byte[]> commands;

        private RedisConnection(final String redisUri) {
            client = RedisClient.create(redisUri);
            connection = client.connect(new Codec());
            commands = connection.sync();
        }

        RedisCommands<String, byte[]> getCommands() {
            return commands;
        }

        @Override
        public void close() {
            commands.close();
            connection.close();
            client.shutdown();
        }
    }

    private AtomicReference<RedisConnection> connectionReference;

    public RedisKeyValueApi(final String name, final String redisUri) {
        Require.nonEmpty(name, "empty name");
        this.name = name;
        this.redisUri = redisUri;
    }

    private RedisCommands<String, byte[]> getCommands() {
        RedisConnection connection = connectionReference.get();
        if (connection == null) {
            throw new NodeUnavailableException();
        }
        return connection.getCommands();
    }

    @Override
    public void put(final String key, final byte[] value) {
        Require.nonEmpty(key, "empty key");
        Require.nonNull(value, "null value");
        try {
            getCommands().set(key, value);
        } catch (RedisException e) {
            throw new NodeOperationException("Unable to SET value in Redis", e);
        }
    }

    @Override
    public Optional<byte[]> get(final String key) {
        Require.nonEmpty(key, "empty key");
        try {
            return Optional.ofNullable(getCommands().get(key));
        } catch (RedisException e) {
            throw new NodeOperationException("Unable to GET value in Redis", e);
        }
    }

    @Override
    public Set<String> getKeys(String prefix) {
        Require.nonNull(prefix, "null prefix");
        try {
            return new HashSet<>(getCommands().keys(prefix));
        } catch (RedisException e) {
            throw new NodeOperationException("Unable to SET value in Redis", e);
        }
    }

    @Override
    public void delete(final String key) {
        Require.nonEmpty(key, "empty key");
        try {
            getCommands().del(key);
        } catch (RedisException e) {
            throw new NodeOperationException("Unable to SET value in Redis", e);
        }
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return Collections.singleton(new NodeInfo(name, connectionReference.get() != null ? NodeStatus.UP : NodeStatus.DOWN));
    }

    @Override
    public void action(String node, NodeAction action) {
        switch (action) {
            case UP:
                if (connectionReference.get() == null) {
                    RedisConnection newConnection = new RedisConnection(redisUri);
                    if (!connectionReference.compareAndSet(null, newConnection)) {
                        newConnection.close();
                    }
                }
                break;
            case DOWN:
                RedisConnection connection = connectionReference.getAndSet(null);
                if (connection != null) {
                    connection.close();
                }
                break;
        }
    }

}
