package ru.csc.bdse.kv.redis;

import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.codec.RedisCodec;
import com.lambdaworks.redis.codec.StringCodec;

import java.nio.ByteBuffer;

public class Codec implements RedisCodec<String, byte[]> {
    private final ByteArrayCodec valCodec;
    private final StringCodec keyCodec;

    public Codec() {
        this.valCodec = new ByteArrayCodec();
        this.keyCodec = new StringCodec();
    }

    @Override
    public String decodeKey(ByteBuffer byteBuffer) {
        return keyCodec.decodeKey(byteBuffer);
    }

    @Override
    public byte[] decodeValue(ByteBuffer byteBuffer) {
        return valCodec.decodeValue(byteBuffer);
    }

    @Override
    public ByteBuffer encodeKey(String s) {
        return keyCodec.encodeKey(s);
    }

    @Override
    public ByteBuffer encodeValue(byte[] bytes) {
        return valCodec.encodeValue(bytes);
    }
}