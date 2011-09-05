package org.ektorp.impl.cluster.state;


public class NodeState {

    private final State state;

    public NodeState(State state) {
        this.state = state;
    }

    public State state(){
        return state;
    }


    public enum State {
        Running, Failed, Recovering
    }

}

