package ru.csc.bdse.kv.partitioned;

import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.partitioning.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class ShardsConfiguration {
    private final Map<String, KeyValueApi> shards;
    private final int shardTimeoutMs;
    private final Partitioner partitioner;

    /**
     * @param shards List of interfaces to shards, the map  is copied into the newly created object.
     */
    public ShardsConfiguration(Map<String, KeyValueApi> shards, int shardTimeoutMs, Partitioner partitioner) {
        this.shards = new HashMap<>(shards);
        this.shardTimeoutMs = shardTimeoutMs;
        this.partitioner = partitioner;
    }

    public Map<String, KeyValueApi> getShards() {
        return shards;
    }

    public int getShardTimeoutMs() {
        return shardTimeoutMs;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }

    @Override
    public String toString() {
        return "ShardsConfiguration{" +
                "shards=" + shards +
                ", shardTimeoutMs=" + shardTimeoutMs +
                ", partitioner=" + partitioner +
                '}';
    }
}
