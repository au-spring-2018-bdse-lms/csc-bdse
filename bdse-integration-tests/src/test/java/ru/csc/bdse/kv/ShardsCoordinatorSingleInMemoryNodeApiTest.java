package ru.csc.bdse.kv;

import ru.csc.bdse.kv.partitioned.ShardsConfiguration;
import ru.csc.bdse.kv.partitioned.ShardsCoordinator;
import ru.csc.bdse.partitioning.ModNPartitioner;

import java.util.Collections;

public class ShardsCoordinatorSingleInMemoryNodeApiTest extends AbstractKeyValueApiTest {
    @Override
    protected KeyValueApi newKeyValueApi() {
        return new ShardsCoordinator(new ShardsConfiguration(
                Collections.singletonMap("node0", new InMemoryKeyValueApi("node0")),
                100,
                new ModNPartitioner(Collections.singleton("node0"))
        ));
    }
}
