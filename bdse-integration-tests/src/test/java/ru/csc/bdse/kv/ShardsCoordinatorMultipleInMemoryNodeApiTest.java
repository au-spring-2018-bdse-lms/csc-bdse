package ru.csc.bdse.kv;

import ru.csc.bdse.kv.partitioned.ShardsConfiguration;
import ru.csc.bdse.kv.partitioned.ShardsCoordinator;
import ru.csc.bdse.partitioning.ModNPartitioner;

import java.util.HashMap;
import java.util.Map;

public class ShardsCoordinatorMultipleInMemoryNodeApiTest extends AbstractKeyValueApiTest {
    @Override
    protected KeyValueApi newKeyValueApi() {
        Map<String, KeyValueApi> shards = new HashMap<>();
        shards.put("node0", new InMemoryKeyValueApi("node0"));
        shards.put("node1", new InMemoryKeyValueApi("node1"));
        shards.put("node2", new InMemoryKeyValueApi("node2"));
        return new ShardsCoordinator(new ShardsConfiguration(
                shards,
                100,
                new ModNPartitioner(shards.keySet())
        ));
    }

    @Override
    protected int numberOfNodes() {
        return 3;
    }
}
