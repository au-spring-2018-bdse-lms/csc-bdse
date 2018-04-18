package ru.csc.bdse.kv.partitioned;

import java.util.Set;

public class UnknownNodeException extends IllegalStateException {
    public UnknownNodeException(String unknownNodeName, Set<String> knownNodes) {
        super("Unknown node " + unknownNodeName + "; expected one of " + knownNodes);
    }
}
