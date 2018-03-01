package ru.csc.bdse.util;

import java.util.Optional;

/**
 * @author semkagtn
 */
public class Env {

    private Env() {

    }

    public static final String KVNODE_NAME = "KVNODE_NAME";

    public static final String KVNODE_INMEMORY = "KVNODE_INMEMORY";

    public static final String KVNODE_REDIS_URI = "KVNODE_REDIS_URI";

    public static Optional<String> get(final String name) {
        return Optional.ofNullable(System.getenv(name));
    }
}
