package ru.csc.bdse.kv;

import ru.csc.bdse.kv.distributed.ClusterConfiguration;
import ru.csc.bdse.kv.distributed.Coordinator;
import ru.csc.bdse.kv.distributed.LastTimestampConflictResolver;

import java.util.Collections;

public class CoordinatorSingleInMemoryNodeApiTest extends AbstractKeyValueApiTest {
    @Override
    protected KeyValueApi newKeyValueApi() {
        return new Coordinator(new ClusterConfiguration(
                Collections.singletonList(new InMemoryKeyValueApi("node")),
                1000,
                1,
                1
        ), new LastTimestampConflictResolver());
    }
}
