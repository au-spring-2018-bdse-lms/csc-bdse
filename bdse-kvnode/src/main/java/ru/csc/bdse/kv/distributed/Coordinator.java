package ru.csc.bdse.kv.distributed;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeUnavailableException;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Coordinator implements KeyValueApi {
    private final ClusterConfiguration configuration;
    private final ConflictResolver resolver;
    private final int replicasCount;

    // Per-replica executor so if requests to a specific replica start
    // hanging, it does not affect requests to other replicas.
    private ExecutorService[] executorServices;

    public Coordinator(ClusterConfiguration configuration, ConflictResolver resolver) {
        this.configuration = configuration;
        this.resolver = resolver;

        replicasCount = configuration.getReplicas().size();
        executorServices = new ExecutorService[replicasCount];
        for (int i = 0; i < replicasCount; i++) {
            executorServices[i] = Executors.newCachedThreadPool();
        }
    }

    @Override
    public void put(String key, byte[] value) {
        byte[] record =
                VersionedRecord.newBuilder()
                        .setPayload(ByteString.copyFrom(value))
                        .setIsDeleted(false)
                        .setTimestamp(System.currentTimeMillis())
                        .build()
                        .toByteArray();
        submitToReplicasAndCheck(configuration.getWriteConsistencyLevel(), replica -> {
            replica.put(key, record);
            return null;
        });
    }

    @Override
    public Optional<byte[]> get(String key) {
        VersionedRecord result =
                resolver.resolve(
                        submitToReplicasAndCheck(configuration.getReadConsistencyLevel(), replica -> {
                            Optional<byte[]> record = replica.get(key);
                            if (record.isPresent()) {
                                try {
                                    return VersionedRecord.parseFrom(record.get());
                                } catch (InvalidProtocolBufferException ignored) {
                                }
                            }
                            return null;
                        }));
        if (result == null || result.getIsDeleted()) {
            return Optional.empty();
        } else {
            return Optional.of(result.getPayload().toByteArray());
        }
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return resolver.resolveKeys(
                submitToReplicasAndCheck(configuration.getReadConsistencyLevel(), replica -> replica.getKeys(prefix)));
    }

    @Override
    public void delete(String key) {
        byte[] record =
                VersionedRecord.newBuilder()
                        .setIsDeleted(true)
                        .setTimestamp(System.currentTimeMillis())
                        .build()
                        .toByteArray();
        submitToReplicasAndCheck(configuration.getWriteConsistencyLevel(), replica -> {
            replica.put(key, record);
            return null;
        });
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return submitToReplicas(replicasCount, replica -> replica.getInfo())
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public void action(String node, NodeAction action) {
        submitToReplicas(replicasCount, replica -> { replica.action(node, action); return null; });
    }

    private <T> List<T> submitToReplicas(int needReplies, Function<KeyValueApi, T> task) {
        List<T> results = new ArrayList<T>();
        CountDownLatch latch = new CountDownLatch(needReplies);
        Future[] futures = new Future[replicasCount];
        for (int i = 0; i < replicasCount; i++) {
            final KeyValueApi replica = configuration.getReplicas().get(i);
            futures[i] = CompletableFuture
                    .supplyAsync(() -> task.apply(replica), executorServices[i])
                    .thenApply(result -> {
                        synchronized (results) {
                            results.add(result);
                        }
                        latch.countDown();
                        return result;
                    });
        }
        try {
            latch.await(configuration.getReplicaTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException ignored) {
            throw new NodeUnavailableException();
        }
        for (Future future : futures) {
            future.cancel(/*mayInterruptIfRunning=*/ true);
        }
        synchronized (results) {
            return new ArrayList<>(results);
        }
    }

    private<T> List<T> submitToReplicasAndCheck(int needReplies, Function<KeyValueApi, T> task) {
        List<T> result = submitToReplicas(needReplies, task);
        if (result.size() < needReplies) {
            throw new NodeUnavailableException();
        } else {
            return result;
        }
    }
}
