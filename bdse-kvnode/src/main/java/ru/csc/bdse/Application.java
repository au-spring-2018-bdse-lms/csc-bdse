package ru.csc.bdse;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import ru.csc.bdse.kv.InMemoryKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.KeyValueApiHttpClient;
import ru.csc.bdse.kv.distributed.ClusterConfiguration;
import ru.csc.bdse.kv.distributed.Coordinator;
import ru.csc.bdse.kv.distributed.LastTimestampConflictResolver;
import ru.csc.bdse.kv.redis.RedisKeyValueApi;
import ru.csc.bdse.util.Env;

import java.util.*;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    private static String randomNodeName() {
        return "kvnode-" + UUID.randomUUID().toString().substring(4);
    }

    @Bean(name="external")
    KeyValueApi external(@Qualifier("inner") KeyValueApi innerNode) {
        String[] replicasUrl = Env.get(Env.KVNODE_REPLICAS_URL).orElse("").split(",");
        if (replicasUrl.length == 0) {
            return innerNode;
        }
        int replicaTimeoutMs = Integer.parseInt(Env.get(Env.KVNODE_REPLICA_TIMEOUT_MS).orElse("1000"));
        int wcl = Integer.parseInt(Env.get(Env.KVNODE_WCL).orElse(Integer.toString(replicasUrl.length)));
        int rcl = Integer.parseInt(Env.get(Env.KVNODE_RCL).orElse(Integer.toString(replicasUrl.length)));

        List<KeyValueApi> peers = new ArrayList<KeyValueApi>();
        peers.add(innerNode);
        for (String peerUrl : replicasUrl) {
            peers.add(new KeyValueApiHttpClient(peerUrl));
        }
        return new Coordinator(new ClusterConfiguration(peers, replicaTimeoutMs, wcl, rcl), new LastTimestampConflictResolver());
    }

    @Bean(name="inner")
    KeyValueApi innerNode() {
        String nodeName = Env.get(Env.KVNODE_NAME).orElseGet(Application::randomNodeName);
        if (!Env.get(Env.KVNODE_INMEMORY).orElse("").equals("")) {
            return new InMemoryKeyValueApi(nodeName);
        }
        String redisUri = Env.get(Env.KVNODE_REDIS_URI).orElse("");
        if (!redisUri.equals("")) {
            return new RedisKeyValueApi(nodeName, redisUri);
        }
        throw new IllegalArgumentException("Neither KVNODE_INMEMORY nor KVNODE_REDIS_URI envvar is specified");
    }
}
