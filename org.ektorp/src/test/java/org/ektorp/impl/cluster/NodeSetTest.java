package org.ektorp.impl.cluster;


import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeSetTest {

    private final PartitionResolver mockPartitionResolver = mock(PartitionResolver.class);

    private final Node node1 = mock(Node.class);
    private final Node node2 = mock(Node.class);
    private final Node node3 = mock(Node.class);
    private final Node node4 = mock(Node.class);

    private NodeSet nodeSet;

    @Before
    public void setUp() throws Exception {
        nodeSet = new NodeSet(mockPartitionResolver, node1, node2, node3, node4);
    }

    @Test
    public void primary_node_for_document_is_return_if_running() {
        when(mockPartitionResolver.partitionIdFor("docid")).thenReturn(2);
        when(node3.isRunning()).thenReturn(true);

        Node result = nodeSet.nodeForId("docid");
        assertEquals(node3, result);
    }

    @Test
    public void random_running_node_is_return_if_primary_not_running() {
        when(mockPartitionResolver.partitionIdFor("docid")).thenReturn(2);
        when(node3.isRunning()).thenReturn(false);

        when(node1.isRunning()).thenReturn(false);
        when(node2.isRunning()).thenReturn(true);
        when(node4.isRunning()).thenReturn(true);

        Node result = nodeSet.nodeForId("docid");

        assertTrue(Arrays.asList(node2, node4).contains(result));
    }

    @Test()
    public void random_recovering_is_returned_if_none_are_running() {
        when(mockPartitionResolver.partitionIdFor("docid")).thenReturn(2);

        when(node1.isRunning()).thenReturn(false);
        when(node2.isRunning()).thenReturn(false);
        when(node3.isRunning()).thenReturn(false);
        when(node4.isRunning()).thenReturn(false);

        when(node1.isRecovering()).thenReturn(true);
        when(node2.isRecovering()).thenReturn(true);
        when(node3.isRecovering()).thenReturn(false);
        when(node4.isRecovering()).thenReturn(false);

        Node result = nodeSet.nodeForId("docid");

        assertTrue(Arrays.asList(node1, node2).contains(result));
    }


    @Test(expected = ShardUnavailableException.class)
    public void an_exception_is_thrown_if_no_node_are_running_or_recovering() {
        when(mockPartitionResolver.partitionIdFor("docid")).thenReturn(2);

        when(node1.isRunning()).thenReturn(false);
        when(node2.isRunning()).thenReturn(false);
        when(node3.isRunning()).thenReturn(false);
        when(node4.isRunning()).thenReturn(false);

        when(node1.isRecovering()).thenReturn(false);
        when(node2.isRecovering()).thenReturn(false);
        when(node3.isRecovering()).thenReturn(false);
        when(node4.isRecovering()).thenReturn(false);

        nodeSet.nodeForId("docid");
    }


}
