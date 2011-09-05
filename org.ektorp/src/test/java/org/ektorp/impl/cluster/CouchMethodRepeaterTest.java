package org.ektorp.impl.cluster;


import org.ektorp.CouchDbConnector;
import org.ektorp.DbAccessException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class CouchMethodRepeaterTest {

    private CouchMethodRepeater couchMethodRepeater;

    private NodeSet mockNodeSet = mock(NodeSet.class);

    @Before
    public void setUp(){
        couchMethodRepeater = new CouchMethodRepeater(mockNodeSet);
    }

    @Test
    public void method_runner_is_called_everything_fine(){
        CouchMethodRunner mockCouchMethodRunner = mock(CouchMethodRunner.class);
        Node mockNode = mock(Node.class);
        CouchDbConnector mockConnector = mock(CouchDbConnector.class);

        when(mockNodeSet.nodeForId("docid")).thenReturn(mockNode);
        when(mockNode.connector()).thenReturn(mockConnector);

        when(mockCouchMethodRunner.run(mockConnector)).thenReturn("a result");

        Object result = couchMethodRepeater.repeat("docid", mockCouchMethodRunner);
        assertEquals("a result", result);
    }

    @Test
    public void method_runner_throw_a_db_access_exception_from_io_exception_then_node_is_marked_failed_next_node_is_tried(){

        CouchMethodRunner mockCouchMethodRunner = mock(CouchMethodRunner.class);
        Node mockNode1 = mock(Node.class);
        CouchDbConnector mockConnector1 = mock(CouchDbConnector.class);
        Node mockNode2 = mock(Node.class);
        CouchDbConnector mockConnector2 = mock(CouchDbConnector.class);

        when(mockNodeSet.nodeForId("docid")).thenReturn(mockNode1, mockNode2);
        when(mockNode1.connector()).thenReturn(mockConnector1);

        when(mockCouchMethodRunner.run(mockConnector1)).thenThrow(new DbAccessException(new IOException()));

        when(mockNode2.connector()).thenReturn(mockConnector2);

        when(mockCouchMethodRunner.run(mockConnector2)).thenReturn("a result");

        Object result = couchMethodRepeater.repeat("docid", mockCouchMethodRunner);
        assertEquals("a result", result);

        verify(mockNode1).markFailed();
    }

    @Test( expected = DbAccessException.class)
    public void method_runner_throw_a_db_access_exception_not_from_io_exception(){

        CouchMethodRunner mockCouchMethodRunner = mock(CouchMethodRunner.class);
        Node mockNode1 = mock(Node.class);
        CouchDbConnector mockConnector1 = mock(CouchDbConnector.class);
        Node mockNode2 = mock(Node.class);
        CouchDbConnector mockConnector2 = mock(CouchDbConnector.class);

        when(mockNodeSet.nodeForId("docid")).thenReturn(mockNode1, mockNode2);
        when(mockNode1.connector()).thenReturn(mockConnector1);

        when(mockCouchMethodRunner.run(mockConnector1)).thenThrow(new DbAccessException());

        when(mockNode2.connector()).thenReturn(mockConnector2);

        when(mockCouchMethodRunner.run(mockConnector2)).thenReturn("a result");

        couchMethodRepeater.repeat("docid", mockCouchMethodRunner);
    }

    @Test(expected = ShardUnavailableException.class)
    public void method_runner_throw_a_db_access_no_more_node_are_available(){

        CouchMethodRunner mockCouchMethodRunner = mock(CouchMethodRunner.class);
        Node mockNode1 = mock(Node.class);
        CouchDbConnector mockConnector1 = mock(CouchDbConnector.class);

        when(mockNodeSet.nodeForId("docid")).thenReturn(mockNode1);
        when(mockNode1.connector()).thenReturn(mockConnector1);

        when(mockCouchMethodRunner.run(mockConnector1)).thenThrow(new DbAccessException());

        when(mockNodeSet.nodeForId("docid")).thenThrow(new ShardUnavailableException());

        couchMethodRepeater.repeat("docid", mockCouchMethodRunner);
    }
    

}
