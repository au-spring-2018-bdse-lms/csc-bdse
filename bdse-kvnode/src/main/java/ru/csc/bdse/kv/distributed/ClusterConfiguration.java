package ru.csc.bdse.kv.distributed;

import ru.csc.bdse.kv.KeyValueApi;

import java.util.ArrayList;
import java.util.List;

/**
 * Immutable cluster configuration.
 */
public class ClusterConfiguration {
    private final List<KeyValueApi> replicas;
    private final int replicaTimeoutMs;
    private final int writeConsistencyLevel;
    private final int readConsistencyLevel;

    /**
     * @param replicas List of interfaces to replicas, the list is copied into the newly created object.
     */
    public ClusterConfiguration(List<KeyValueApi> replicas, int replicaTimeoutMs, int writeConsistencyLevel, int readConsistencyLevel) {
        this.replicas = new ArrayList<>(replicas);
        this.replicaTimeoutMs = replicaTimeoutMs;
        this.writeConsistencyLevel = writeConsistencyLevel;
        this.readConsistencyLevel = readConsistencyLevel;
    }

    public List<KeyValueApi> getReplicas() {
        return replicas;
    }

    public int getReplicaTimeoutMs() {
        return replicaTimeoutMs;
    }

    public int getWriteConsistencyLevel() {
        return writeConsistencyLevel;
    }

    public int getReadConsistencyLevel() {
        return readConsistencyLevel;
    }

    @Override
    public String toString() {
        return "ClusterConfiguration{" +
                "replicas=" + replicas +
                ", replicaTimeoutMs=" + replicaTimeoutMs +
                ", writeConsistencyLevel=" + writeConsistencyLevel +
                ", readConsistencyLevel=" + readConsistencyLevel +
                '}';
    }
}
