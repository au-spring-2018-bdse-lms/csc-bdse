package ru.csc.bdse.kv.redis;

import java.util.*;

import com.google.common.collect.ImmutableMap;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.*;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.catalina.Host;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeStatus;
import ru.csc.bdse.util.Require;

public class KeyValueRedisInsideApi implements KeyValueApi {
    private final String buildContainerName = "someredisbuilded";
    private final String containerName = buildContainerName;
    private final String imageName = "darkpeaceduck/bdse:redis_only";
    private final DockerClient docker;
    private final String redisPortInsideContainer = "6379/tcp";
    private String redisHostPort = null;
    private NodeStatus status;

    private ContainerConfig continerConfig = null;

    public KeyValueRedisInsideApi() throws DockerCertificateException {
        docker = new DefaultDockerClient("unix:///var/run/docker.sock");
    }


    @Override
    public void put(String key, byte[] value) {

    }

    @Override
    public Optional<byte[]> get(String key) {
        return Optional.empty();
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return null;
    }

    @Override
    public void delete(String key) {

    }

    @Override
    public Set<NodeInfo> getInfo() {
        try {
            return Collections.singleton(new NodeInfo("", getStatus()));
        } catch (DockerException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void action(String node, NodeAction action) {
        Require.nonNull(action, "action is null");
        if (action == NodeAction.UP) {
            try {
                upRedis();
            } catch (DockerException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
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
        final ContainerInfo info = docker.inspectContainer(containerName);
        if (info.state().running()) {
            redisHostPort = info.networkSettings().ports().get(redisPortInsideContainer).get(0).hostPort();
            return NodeStatus.UP;
        } else
            return NodeStatus.DOWN;
    }

}
