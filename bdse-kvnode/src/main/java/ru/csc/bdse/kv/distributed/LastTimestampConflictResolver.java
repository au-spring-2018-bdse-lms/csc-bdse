package ru.csc.bdse.kv.distributed;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LastTimestampConflictResolver implements ConflictResolver {
    @Override
    public VersionedRecord resolve(List<VersionedRecord> records) {
        VersionedRecord result = null;
        for (VersionedRecord record : records) {
            if (record == null) continue;
            // TODO: what if timestamps coincide?
            if (result == null || result.getTimestamp() < record.getTimestamp()) {
                result = record;
            }
        }
        return result;
    }

    @Override
    public Set<String> resolveKeys(List<Set<String>> keys) {
        return keys.stream().flatMap(Collection::stream).collect(Collectors.toSet());
    }
}
