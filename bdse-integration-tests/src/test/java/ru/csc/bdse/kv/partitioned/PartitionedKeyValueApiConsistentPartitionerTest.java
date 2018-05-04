package ru.csc.bdse.kv.partitioned;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.BeforeClass;
import ru.csc.bdse.kv.InMemoryKeyValueApi;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.partitioning.ConsistentHashMd5Partitioner;
import ru.csc.bdse.partitioning.Partitioner;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PartitionedKeyValueApiConsistentPartitionerTest extends AbstractPartitionedKeyValueApiHttpClientTest {
    private static KeyValueApi[] innerNodes;
    private static Map<String, KeyValueApi> shardsForCluster1;
    private static Map<String, KeyValueApi> shardsForCluster2;
    private static Partitioner partitionerForCluster1;
    private static Partitioner partitionerForCluster2;
    private static Set<String> keys = Stream.generate(() -> RandomStringUtils.randomAlphanumeric(10)).limit(1000)
            .collect(Collectors.toSet());

    @BeforeClass
    public static void setUp() {
        innerNodes = new KeyValueApi[3];
        for (int i = 0; i < 3; i++) {
            innerNodes[i] = new InMemoryKeyValueApi("node" + i);
        }
        shardsForCluster1 = new HashMap<>();
        for (int i = 0; i < 3; i++) {
            shardsForCluster1.put("node" + i, innerNodes[i]);
        }
        shardsForCluster2 = new HashMap<>();
        shardsForCluster2.put("node0", innerNodes[0]);
        shardsForCluster2.put("node2", innerNodes[2]);
        partitionerForCluster1 = new ConsistentHashMd5Partitioner(shardsForCluster1.keySet());
        partitionerForCluster2 = new ConsistentHashMd5Partitioner(shardsForCluster2.keySet());
    }

    @Override
    protected KeyValueApi newCluster1() {
        return new ShardsCoordinator(new ShardsConfiguration(
                shardsForCluster1,
                3000,
                partitionerForCluster1
        ));
    }

    @Override
    protected KeyValueApi newCluster2() {
        return new ShardsCoordinator(new ShardsConfiguration(
                shardsForCluster2,
                3000,
                partitionerForCluster2
        ));
    }

    @Override
    protected Set<String> keys() {
        return keys;
    }

    @Override
    protected float expectedKeysLossProportion() {
        return (float) innerNodes[1].getKeys("").size() / keys.size();
    }

    @Override
    protected float expectedUndeletedKeysProportion() {
        return (float) innerNodes[1].getKeys("").size() / keys.size();
    }
}
