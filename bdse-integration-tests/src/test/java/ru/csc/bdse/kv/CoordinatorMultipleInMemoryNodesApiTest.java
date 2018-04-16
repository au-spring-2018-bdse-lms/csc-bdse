package ru.csc.bdse.kv;

import org.junit.Test;
import ru.csc.bdse.kv.distributed.ClusterConfiguration;
import ru.csc.bdse.kv.distributed.Coordinator;
import ru.csc.bdse.kv.distributed.LastTimestampConflictResolver;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class CoordinatorMultipleInMemoryNodesApiTest extends AbstractKeyValueApiTest {
    @Override
    protected KeyValueApi newKeyValueApi() {
        return new Coordinator(new ClusterConfiguration(
                Arrays.asList(
                        new InMemoryKeyValueApi("node-0"),
                        new InMemoryKeyValueApi("node-1"),
                        new InMemoryKeyValueApi("node-2")
                ),
                1000,
                2,
                2
        ), new LastTimestampConflictResolver());
    }

    @Override
    protected int numberOfNodes() {
        return 3;
    }
}
