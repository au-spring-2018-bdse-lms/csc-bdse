package ru.csc.bdse.kv;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

public class FailoverKeyValueApi implements KeyValueApi {
    final List<KeyValueApi> backends;
    int lastBackend = 0;

    public FailoverKeyValueApi(List<KeyValueApi> backends) {
        this.backends = backends;
    }

    @Override
    public void put(String key, byte[] value) {
        retry(backend -> { backend.put(key, value); return null; });
    }

    @Override
    public Optional<byte[]> get(String key) {
        return retry(backend -> backend.get(key));
    }

    @Override
    public Set<String> getKeys(String prefix) {
        return retry(backend -> backend.getKeys(prefix));
    }

    @Override
    public void delete(String key) {
        retry(backend -> { backend.delete(key); return null; });
    }

    @Override
    public Set<NodeInfo> getInfo() {
        return retry(backend -> backend.getInfo());
    }

    @Override
    public void action(String node, NodeAction action) {
        retry(backend -> { backend.action(node, action); return null; });
    }

    protected <T> T retry(Function<KeyValueApi, T> task) {
        for (int i = 0; i < backends.size(); i++) {
            try {
                return task.apply(backends.get(lastBackend));
            } catch (Exception ignored) {
            }
            lastBackend = (lastBackend + 1) % backends.size();
        }
        throw new NodeUnavailableException();
    }
}
