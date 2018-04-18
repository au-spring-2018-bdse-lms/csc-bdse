package ru.csc.bdse.kv.partitioned;

import ru.csc.bdse.kv.KeyValueApi;
import ru.csc.bdse.kv.NodeAction;
import ru.csc.bdse.kv.NodeInfo;
import ru.csc.bdse.kv.NodeUnavailableException;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ShardsCoordinator implements KeyValueApi {
    private final ShardsConfiguration configuration;

    // Per-replica executor so if requests to a specific replica start
    // hanging, it does not affect requests to other replicas.
    private Map<String, ExecutorService> executorServices;

    public ShardsCoordinator(ShardsConfiguration configuration) {
        this.configuration = configuration;
        executorServices = new HashMap<>();
        for (String key : configuration.getShards().keySet()) {
            executorServices.put(key, Executors.newCachedThreadPool());
        }
    }

    @Override
    public void put(String key, byte[] value) {
        execute(configuration.getPartitioner().getPartition(key),
                shard -> {
                    shard.put(key, value);
                    return null;
                });
    }

    @Override
    public Optional<byte[]> get(String key) {
        return execute(configuration.getPartitioner().getPartition(key),
                shard -> shard.get(key));
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return executeOnAll(shard -> shard.getKeys(prefix)).stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public void delete(String key) {
        execute(configuration.getPartitioner().getPartition(key),
                shard -> {
                    shard.delete(key);
                    return null;
                });
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return executeOnAll(KeyValueApi::getInfo).stream()
                .flatMap(Set::stream)
                .collect(Collectors.toSet());
    }

    @Override
    public void action(String node, NodeAction action) {
        execute(node, shard -> {
            shard.action(node, action);
            return null;
        });
    }

    private <T> T execute(String node, Function<KeyValueApi, T> task) {
        KeyValueApi api = configuration.getShards().get(node);
        if (api == null) {
            throw new UnknownNodeException(node, configuration.getShards().keySet());
        }
        try {
            return executorServices.get(node)
                    .submit(() -> task.apply(api))
                    .get(configuration.getShardTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new NodeUnavailableException("Request to underlying shard failed", e);
        } catch (TimeoutException e) {
            throw new NodeUnavailableException("Request timed out", e);
        }
    }

    private <T> List<T> executeOnAll(Function<KeyValueApi, T> task) {
        List<CompletableFuture<T>> futures = new ArrayList<>();
        for (Map.Entry<String, KeyValueApi> shardEntry : configuration.getShards().entrySet()) {
            futures.add(CompletableFuture.supplyAsync(
                    () -> task.apply(shardEntry.getValue()),
                    executorServices.get(shardEntry.getKey())
            ));
        }
        CompletableFuture<Void> allFuturesCompletion =
                CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        try {
            allFuturesCompletion.get(configuration.getShardTimeoutMs(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException e) {
            throw new NodeUnavailableException("Request to underlying shard failed", e);
        } catch (TimeoutException e) {
            throw new NodeUnavailableException("Request timed out", e);
        } finally {
            futures.forEach(f -> f.cancel(/* mayInterruptIfRunning= */ true));
        }
        return futures.stream().map(CompletableFuture::join).collect(Collectors.toList());
    }
}
