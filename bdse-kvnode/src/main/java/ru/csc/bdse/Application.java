package ru.csc.bdse;

import com.spotify.docker.client.exceptions.DockerCertificateException;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.csc.bdse.kv.InMemoryKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.redis.KeyValueRedisInsideApi;
import ru.csc.bdse.util.Env;

import java.util.UUID;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    private static String randomNodeName() {
        return "kvnode-" + UUID.randomUUID().toString().substring(4);
    }

    @Bean
    KeyValueApi node() throws DockerCertificateException {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(Application::randomNodeName);
//        return new InMemoryKeyValueApi(nodeName);
        return new KeyValueRedisInsideApi(nodeName);
    }
}
