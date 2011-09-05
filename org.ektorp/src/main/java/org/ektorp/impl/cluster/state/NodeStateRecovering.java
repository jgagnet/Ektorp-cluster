package org.ektorp.impl.cluster.state;


public class NodeStateRecovering extends NodeState{

    private final long recoveryPeriod;
    private final long recoveredAt = System.currentTimeMillis();

    public NodeStateRecovering(long recoveryPeriod) {
        super(State.Recovering);
        this.recoveryPeriod = recoveryPeriod;
    }

    public boolean hasRecover() {
        return (System.currentTimeMillis() - recoveredAt) > recoveryPeriod;
    }



}
