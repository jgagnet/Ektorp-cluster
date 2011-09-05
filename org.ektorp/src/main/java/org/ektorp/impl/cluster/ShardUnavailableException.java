package org.ektorp.impl.cluster;


public class ShardUnavailableException extends RuntimeException {

    public ShardUnavailableException() {
        super();
    }

    public ShardUnavailableException(String s) {
        super(s);
    }

    public ShardUnavailableException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public ShardUnavailableException(Throwable throwable) {
        super(throwable);
    }
}
