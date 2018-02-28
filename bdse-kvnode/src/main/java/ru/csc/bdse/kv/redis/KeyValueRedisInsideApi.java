package ru.csc.bdse.kv.redis;

import java.nio.ByteBuffer;
import java.util.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisURI;
import com.lambdaworks.redis.api.StatefulRedisConnection;
import com.lambdaworks.redis.api.sync.RedisCommands;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.StringCodec;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.ContainerNotFoundException;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.*;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeStatus;
import ru.csc.bdse.util.Require;
import com.lambdaworks.redis.codec.ByteArrayCodec;

public class KeyValueRedisInsideApi implements KeyValueApi {
    private final String buildContainerName = "someredisbuilded";
    private final String containerName = buildContainerName;
    private final String imageName = "darkpeaceduck/bdse:redis_only";
    private final String redisPortInsideContainer = "6379/tcp";


    private class RunningState {
        public class Codec implements RedisCodec<String, byte[]> {
            ByteArrayCodec valCodec;
            StringCodec keyCodec;

            public Codec() {
                this.valCodec = new ByteArrayCodec();
                this.keyCodec = new StringCodec();
            }

            @Override
            public String decodeKey(ByteBuffer byteBuffer) {
                return keyCodec.decodeKey(byteBuffer);
            }

            @Override
            public byte[] decodeValue(ByteBuffer byteBuffer) {
                return valCodec.decodeValue(byteBuffer);
            }

            @Override
            public ByteBuffer encodeKey(String s) {
                return keyCodec.encodeKey(s);
            }

            @Override
            public ByteBuffer encodeValue(byte[] bytes) {
                return valCodec.encodeValue(bytes);
            }
        }
        String redisHostPort = null;
        RedisClient client = null;
        StatefulRedisConnection<String, byte[]> connection = null;
        RedisCommands<String, byte[]> commands = null;

        void updateOnStarted(final ContainerInfo info) {
            redisHostPort = info.networkSettings().ports().get(redisPortInsideContainer).get(0).hostPort();
            client = RedisClient.create(RedisURI.create("localhost",
                    Integer.parseInt(redisHostPort)));
            connection = client.connect(new Codec());
            commands = connection.sync();
        }

        void updateOnStopped() {
            if (connection != null)
                connection.close();
            if (client != null)
                client.shutdown();

            redisHostPort = null;
            client = null;
            connection = null;
            commands = null;
        }
        RedisCommands<String, byte[]> getCommands() {
            if (commands == null)
                throw new IllegalArgumentException();
            return commands;
        }
    }

    private RunningState rstate;
    private final String nodeName;
    private final DockerClient docker;

    private ContainerConfig continerConfig = null;

    public KeyValueRedisInsideApi(String nodeName) throws DockerCertificateException, DockerException, InterruptedException {
        this.nodeName = nodeName;
        this.docker = new DefaultDockerClient("unix:///var/run/docker.sock");
        this.rstate = new RunningState();

        getStatus();
        action(nodeName, NodeAction.UP);
    }


    @Override
    public void put(String key, byte[] value) {
        try {
            getStatus();
            rstate.getCommands().set(key, value);
        } catch (Exception e) {

        }


    }

    @Override
    public Optional<byte[]> get(String key) {
        try {
            getStatus();
            byte[] val = rstate.getCommands().get(key);
            if (val == null)
                return Optional.empty();
            else
                return Optional.of(val);
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    @Override
    public Set<String> getKeys(String prefix) {
        try {
            getStatus();
            return ImmutableSet.copyOf(rstate.getCommands().keys(prefix));
        } catch (Exception e) {
            return ImmutableSet.of();
        }
    }

    @Override
    public void delete(String key) {
        try {
            getStatus();
            rstate.getCommands().del(key);
        } catch (Exception e) {

        }
    }

    @Override
    public Set<NodeInfo> getInfo() {
        try {
            return Collections.singleton(new NodeInfo(nodeName, getStatus()));
        } catch (DockerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return Collections.singleton(new NodeInfo("FAILED", NodeStatus.DOWN));
    }

    @Override
    public void action(String node, NodeAction action) {
        Require.nonNull(action, "action is null");

        try {
            final NodeStatus status = getStatus();
            if (action == NodeAction.UP && status == NodeStatus.DOWN) {
                upRedis();
                getStatus();
            }
            if (action == NodeAction.DOWN && status == NodeStatus.UP) {
                stopRedis();
                getStatus();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void upRedis() throws DockerException, InterruptedException {
        docker.startContainer(containerName);
    }

    public void stopRedis() throws DockerException, InterruptedException {
        docker.killContainer(containerName);
    }

    public void buildRedis() throws DockerException, InterruptedException {
        {
            ImageInfo info = null;
            final int retryesN = 2;
            for (int retryes = 0; retryes < retryesN; retryes++) {
                try {
                    info = docker.inspectImage(imageName);
                } catch (DockerException | InterruptedException e) {
                    e.printStackTrace();
                    if (retryes < retryesN - 1) {
                        try {
                            docker.pull(imageName);
                        } catch (Exception e1) {
                            e1.printStackTrace();
                        }
                    }
                }
            }
            Require.nonNull(info, "unable to inspect image to build container");
        }

        if (continerConfig == null) {
            final Map<String, List<PortBinding>> portBindings =
                    ImmutableMap.of( redisPortInsideContainer,
                            Arrays.asList(PortBinding.randomPort("0.0.0.0") ) );
            final HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();
            continerConfig = ContainerConfig.builder()
                    .hostConfig(hostConfig)
                    .image(imageName)
                    .attachStderr(Boolean.FALSE)
                    .attachStdin(Boolean.FALSE)
                    .attachStdout(Boolean.FALSE)
                    .tty(Boolean.FALSE)
                    .cmd("redis-server", "--appendonly", "yes")
                    .build();

            Require.nonNull(continerConfig, "unable to builder creation container config");
        }

        docker.createContainer(continerConfig, buildContainerName);
    }

    public NodeStatus getStatus() throws DockerException, InterruptedException {
        ContainerInfo info = null;
        try {
            info = docker.inspectContainer(containerName);
        } catch (ContainerNotFoundException e) {
            buildRedis();
            info = docker.inspectContainer(containerName);
        }
        Require.nonNull(info, "container info is empty");
        if (info.state().running()) {
            rstate.updateOnStarted(info);
            return NodeStatus.UP;
        } else {
            rstate.updateOnStopped();
            return NodeStatus.DOWN;
        }
    }
}
