package ru.csc.bdse.kv.distributed;

import java.util.List;
import java.util.Set;

public interface ConflictResolver {
    public VersionedRecord resolve(List<VersionedRecord> records);

    public Set<String> resolveKeys(List<Set<String>> keys);
}
