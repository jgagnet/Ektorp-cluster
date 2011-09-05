package org.ektorp.impl.cluster;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class NodeSet {

    private final List<Node> nodes;
    private final PartitionResolver partitionResolver;

    public NodeSet(Node... nodes) {
        this(new PartitionResolver(nodes.length), nodes);
    }

    public NodeSet(PartitionResolver partitionResolver, Node... nodes) {
        this.nodes = Arrays.asList(nodes);
        this.partitionResolver = partitionResolver;
    }

    public Node nodeForId(String id) {
        Node primaryNode = primaryNode(id);
        if (primaryNode != null){
            return primaryNode;
        }

        Node runningNode = randomRunningNode();
        if (runningNode != null){
            return runningNode;
        }

        Node recoveringNode = randomRecoveringNode();
        if (recoveringNode != null){
            return recoveringNode;
        }

        throw new ShardUnavailableException("Shard doesn't contains any running node.");

    }
    public void monitorNodes() {
        for (int i=0; i<nodes.size(); i++){
            nodes.get(i).monitorNode();
        }

    }
    

    private Node primaryNode(String id){
        String reverseId = new StringBuffer(id).reverse().toString();
        int partitionId = partitionResolver.partitionIdFor(reverseId);
        Node node = nodes.get(partitionId);
        if (node.isRunning()) {
            return node;
        } else {
            return null;
        }
    }

    private Node randomRunningNode() {
        List<Node> availableNodes = availableNodes();
        if (availableNodes.size() > 0) {
            int nodePosition = new Random().nextInt(availableNodes.size());
            return availableNodes.get(nodePosition);
        } else {
            return null;
        }
    }

    private Node randomRecoveringNode() {
        List<Node> recoveringNodes = recoveringNodes();
        if (recoveringNodes.size() > 0) {
            int nodePosition = new Random().nextInt(recoveringNodes.size());
            return recoveringNodes.get(nodePosition);
        } else {
            return null;
        }
    }

    private List<Node> availableNodes() {
        List<Node> result = new ArrayList<Node>();
        for (Node node : nodes) {
            if (node.isRunning()) {
                result.add(node);
            }
        }
        return result;
    }


    private List<Node> recoveringNodes() {
        List<Node> result = new ArrayList<Node>();
        for (Node node : nodes) {
            if (node.isRecovering()) {
                result.add(node);
            }
        }
        return result;
    }

}
