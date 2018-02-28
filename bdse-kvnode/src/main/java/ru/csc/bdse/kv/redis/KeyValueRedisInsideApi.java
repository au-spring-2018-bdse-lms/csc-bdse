package ru.csc.bdse.kv.redis;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.ContainerInfo;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeStatus;
import ru.csc.bdse.util.Require;

public class KeyValueRedisInsideApi implements KeyValueApi {
    private final String containerName = "some-redis";
    private final DockerClient docker;
    private NodeStatus status;

    public KeyValueRedisInsideApi() throws DockerCertificateException {
//        docker = DefaultDockerClient.builder()
//                .build();
//        docker = DefaultDockerClient.fromEnv().build();
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

    public NodeStatus getStatus() throws DockerException, InterruptedException {
        final ContainerInfo info = docker.inspectContainer(containerName);
        if (info.state().running()) {
            return NodeStatus.UP;
        } else
            return NodeStatus.DOWN;
    }

}
