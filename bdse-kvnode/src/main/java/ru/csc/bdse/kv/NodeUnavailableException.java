package ru.csc.bdse.kv;

public class NodeUnavailableException extends NodeOperationException {
    public NodeUnavailableException() {
        super();
    }

    public NodeUnavailableException(String message) {
        super(message);
    }

    public NodeUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
