package ru.csc.bdse;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.csc.bdse.kv.InMemoryKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.redis.RedisKeyValueApi;
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
    KeyValueApi node() {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(Application::randomNodeName);
        if (Env.get(Env.KVNODE_INMEMORY).orElse("") != "") {
            return new InMemoryKeyValueApi(nodeName);
        }
        String redisUri = Env.get(Env.KVNODE_REDIS_URI).orElse("");
        if (redisUri != "") {
            return new RedisKeyValueApi(nodeName, redisUri);
        }
        throw new IllegalArgumentException("Neither KVNODE_INMEMORY nor KVNODE_REDIS_URI envvar is specified");
    }
}
