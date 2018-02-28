package ru.csc.bdse.kv;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import ru.csc.bdse.util.Env;

import java.io.File;
import java.time.Duration;

import static java.time.temporal.ChronoUnit.SECONDS;

public class DockerInsideDockerTest {
    @ClassRule
    public static final GenericContainer node = new GenericContainer(
            new ImageFromDockerfile()
                    .withFileFromFile("target/bdse-kvnode-0.0.1-SNAPSHOT.jar", new File
                            ("../bdse-kvnode/target/bdse-kvnode-0.0.1-SNAPSHOT.jar"))
                    .withFileFromClasspath("Dockerfile", "kvnode/Dockerfile"))
            .withEnv(Env.KVNODE_NAME, "node-0")
            .withExposedPorts(8080)
            .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock",
                    BindMode.READ_WRITE)
            .withStartupTimeout(Duration.of(30, SECONDS));

    @Test
    public void empty() {
        final String baseUrl = "http://localhost:" + node.getMappedPort(8080);
        KeyValueApi api = new KeyValueApiHttpClient(baseUrl);
        Assert.assertEquals(NodeStatus.UP, api.getInfo().iterator().next().getStatus());
    }
}
