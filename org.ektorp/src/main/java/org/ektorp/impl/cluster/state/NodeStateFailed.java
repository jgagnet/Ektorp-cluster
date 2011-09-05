package org.ektorp.impl.cluster.state;


public class NodeStateFailed extends NodeState{

    private final long failedAt = System.currentTimeMillis();

    public NodeStateFailed() {
        super(State.Failed);
    }

    public long hasFailedFor() {
        return System.currentTimeMillis() - failedAt;
    }

}
