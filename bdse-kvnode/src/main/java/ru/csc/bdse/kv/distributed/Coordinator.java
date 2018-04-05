package ru.csc.bdse.kv.distributed;

import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeUnavailableException;

import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

public class Coordinator implements KeyValueApi {
    private ClusterConfiguration configuration;
    private final int replicasCount;

    // Per-replica executor so if requests to a specific replica start
    // hanging, it does not affect requests to other replicas.
    private ExecutorService[] executorServices;

    public Coordinator(ClusterConfiguration configuration) {
        this.configuration = configuration;
        replicasCount = configuration.getReplicas().size();

        executorServices = new ExecutorService[replicasCount];
        for (int i = 0; i < replicasCount; i++) {
            executorServices[i] = Executors.newSingleThreadExecutor();
        }
    }

    @Override
    public void put(String key, byte[] value) {

    }

    @Override
    public Optional<byte[]> get(String key) {
        return Optional.empty();
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return null;
    }

    @Override
    public void delete(String key) {

    }

    @Override
    public Set<NodeInfo> getInfo() {
        return null;
    }

    @Override
    public void action(String node, NodeAction action) {

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
            synchronized (results) {
                return new ArrayList<>(results);
            }
        } catch (InterruptedException e) {
            throw new NodeUnavailableException();
        } finally {
            for (Future future : futures) {
                future.cancel(/*mayInterruptIfRunning=*/ true);
            }
        }
    }
}
