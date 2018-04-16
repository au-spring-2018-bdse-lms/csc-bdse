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

    public static final String KVNODE_REPLICAS_URL = "KVNODE_REPLICAS_URL";

    public static final String KVNODE_REPLICA_TIMEOUT_MS = "KVNODE_REPLICA_TIMEOUT_MS";

    public static final String KVNODE_WCL = "KVNODE_WCL";

    public static final String KVNODE_RCL = "KVNODE_RCL";

    public static Optional<String> get(final String name) {
        return Optional.ofNullable(System.getenv(name));
    }
}
