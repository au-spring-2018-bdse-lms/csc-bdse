package ru.csc.bdse.app;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.csc.bdse.kv.KeyValueApi;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class PhoneBookKvNodeBase<R extends Record & MessageLite> implements PhoneBookApi<R> {
    private final Logger LOG = LoggerFactory.getLogger(PhoneBookKvNodeBase.class);
    protected final KeyValueApi keyValueApi;
    protected final Parser<R> parser;

    protected PhoneBookKvNodeBase(KeyValueApi keyValueApi, Parser<R> parser) {
        this.keyValueApi = keyValueApi;
        this.parser = parser;
    }

    @Override
    public void put(R record) {
        byte[] data = record.toByteArray();
        for (Character c : record.literals()) {
            keyValueApi.put(c + "@" + record.getUid().toString(), data);
        }
    }

    @Override
    public void delete(R record) {
        for (Character c : record.literals()) {
            keyValueApi.delete(c + "@" + record.getUid().toString());
        }
    }

    @Override
    public Set<R> get(char literal) {
        Set<R> result = new HashSet<>();
        for (String key : keyValueApi.getKeys(literal + "@")) {
            Optional<byte[]> encoded = keyValueApi.get(key);
            if (encoded.isPresent()) {
                try {
                    result.add(parser.parseFrom(encoded.get()));
                } catch (InvalidProtocolBufferException e) {
                    LOG.warn("Unable to parse data from the node", e);
                }
            }
        }
        return result;
    }
}
