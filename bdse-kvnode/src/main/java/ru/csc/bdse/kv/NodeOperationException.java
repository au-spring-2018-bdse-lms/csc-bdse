package ru.csc.bdse.kv;

public class NodeOperationException extends RuntimeException {
    public NodeOperationException() {
        super();
    }

    public NodeOperationException(String message) {
        super(message);
    }

    public NodeOperationException(String message, Throwable cause) {
        super(message, cause);
    }
}
