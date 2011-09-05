package org.ektorp.impl.cluster;


import org.ektorp.CouchDbConnector;
import org.ektorp.http.HttpClient;
import org.ektorp.http.HttpResponse;
import org.ektorp.impl.cluster.state.NodeState;
import org.ektorp.impl.cluster.state.NodeStateFailed;
import org.ektorp.impl.cluster.state.NodeStateRecovering;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeTest {

    private CouchDbConnector mockCouchDbConnector = mock(CouchDbConnector.class);

    @Test
    public void test_is_running(){
        Node node = new Node(mockCouchDbConnector);
        assertEquals(true, node.isRunning());
    }

    @Test
    public void test_is_running_in_recovery_mode(){
        NodeStateRecovering nodeState = mock(NodeStateRecovering.class);
        when(nodeState.state()).thenReturn(NodeState.State.Recovering);
        when(nodeState.hasRecover()).thenReturn(false);

        Node node = new Node(mockCouchDbConnector, nodeState);
        assertEquals(false, node.isRunning());
    }

    @Test
    public void test_is_running_in_recovery_mode_and_has_recovered(){
        NodeStateRecovering nodeState = mock(NodeStateRecovering.class);
        when(nodeState.state()).thenReturn(NodeState.State.Recovering);
        when(nodeState.hasRecover()).thenReturn(true);

        Node node = new Node(mockCouchDbConnector, nodeState);
        assertEquals(true, node.isRunning());
    }


    @Test
    public void monitor_failed_node_check_pass_goes_in_recovery(){
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse mockHttpResponse = mock(HttpResponse.class);

        when(mockCouchDbConnector.getConnection()).thenReturn(mockHttpClient);
        when(mockHttpClient.head("/")).thenReturn(mockHttpResponse);

        Node node = new Node(mockCouchDbConnector, new NodeStateFailed());
        node.monitorNode();
        assertTrue(node.isRecovering());
    }

    @Test
    public void monitor_failed_node_check_failed(){
        HttpClient mockHttpClient = mock(HttpClient.class);

        when(mockCouchDbConnector.getConnection()).thenReturn(mockHttpClient);
        when(mockHttpClient.head("/")).thenThrow(new RuntimeException());

        Node node = new Node(mockCouchDbConnector, new NodeStateFailed());
        node.monitorNode();
        assertFalse(node.isRecovering());
    }


    @Test
    public void monitor_revoring_node_hasnt_recovered(){
        NodeStateRecovering mockNodeStateRecovering = mock(NodeStateRecovering.class);
        when(mockNodeStateRecovering.hasRecover()).thenReturn(false);
        when(mockNodeStateRecovering.state()).thenReturn(NodeState.State.Recovering);

        Node node = new Node(mockCouchDbConnector, mockNodeStateRecovering);

        node.monitorNode();
        assertTrue(node.isRecovering());
    }

    @Test
    public void monitor_revoring_node_recovered(){
        NodeStateRecovering mockNodeStateRecovering = mock(NodeStateRecovering.class);
        when(mockNodeStateRecovering.hasRecover()).thenReturn(true);
        when(mockNodeStateRecovering.state()).thenReturn(NodeState.State.Recovering);

        Node node = new Node(mockCouchDbConnector, mockNodeStateRecovering);

        node.monitorNode();
        assertFalse(node.isRecovering());
    }

}
