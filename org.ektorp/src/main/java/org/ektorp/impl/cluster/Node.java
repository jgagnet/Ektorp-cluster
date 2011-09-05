package org.ektorp.impl.cluster;


import org.ektorp.CouchDbConnector;
import org.ektorp.impl.cluster.state.NodeState;
import org.ektorp.impl.cluster.state.NodeStateFailed;
import org.ektorp.impl.cluster.state.NodeStateRecovering;
import org.ektorp.impl.cluster.state.NodeStateRunning;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Node {

    private final static Logger logger = LoggerFactory.getLogger(Node.class);

    private final CouchDbConnector couchDbConnector;

    private NodeState nodeState;


    public Node(CouchDbConnector couchDbConnector) {
        this(couchDbConnector, new NodeStateRunning());
    }

    protected Node(CouchDbConnector couchDbConnector, NodeState nodeState) {
        this.couchDbConnector = couchDbConnector;
        this.nodeState = nodeState;
    }

    public CouchDbConnector connector() {
        return couchDbConnector;
    }

    public void monitorNode() {
        if (nodeState.state() == NodeState.State.Failed){
            try {
                couchDbConnector.getConnection().head("/");
            } catch (Exception expected) {
                logger.info("Node "+this+" is still unavailable");
                return;
            }
            logger.info("Node "+this+" is now recovering");
            nodeState = new NodeStateRecovering(recoveryPeriod);
        } else if (nodeState.state() == NodeState.State.Recovering){
            NodeStateRecovering nodeStateRecovering = (NodeStateRecovering) nodeState;
            if (nodeStateRecovering.hasRecover()){
                logger.info("Node "+this+" is running");
                nodeState = new NodeStateRunning();
            }
        }
    }

    public boolean isRunning(){
        if (nodeState.state() == NodeState.State.Running){
            return true;
        }
        if (nodeState.state() == NodeState.State.Recovering){
            NodeStateRecovering nodeStateRecovering = (NodeStateRecovering) nodeState;
            if (nodeStateRecovering.hasRecover()){
                nodeState = new NodeStateRunning();
                return true;
            }
        }
        return false;
    }

    public boolean isRecovering() {
        return (nodeState.state() == NodeState.State.Recovering);
    }

    public void markFailed() {
        nodeState = new NodeStateFailed();
    }

    @Override
    public String toString(){
        return String.format("Node[path:%s]", couchDbConnector.path());
    }

}
