package org.ektorp.impl.cluster;


public class NodeNotRunningException extends RuntimeException {

    public NodeNotRunningException() {
        super();
    }

    public NodeNotRunningException(String s) {
        super(s);
    }

    public NodeNotRunningException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public NodeNotRunningException(Throwable throwable) {
        super(throwable);
    }
}
