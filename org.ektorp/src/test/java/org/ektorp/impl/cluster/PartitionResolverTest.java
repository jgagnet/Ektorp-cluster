package org.ektorp.impl.cluster;


import org.junit.Test;
import static org.junit.Assert.*;

public class PartitionResolverTest {


    @Test
    public void partition_of_1_node_should_allways_return_0(){
        PartitionResolver partitionResolver = new PartitionResolver(1);

        assertEquals(0, partitionResolver.partitionIdFor(""));
        assertEquals(0, partitionResolver.partitionIdFor("CouchDB"));
        assertEquals(0, partitionResolver.partitionIdFor("Ektorp"));
        assertEquals(0, partitionResolver.partitionIdFor("Ektorp-cluster"));

    }

    @Test
    public void partition_of_3_node_should_follow_lounge_algorithm(){
        PartitionResolver partitionResolver = new PartitionResolver(3);

        assertEquals(0, partitionResolver.partitionIdFor(""));
        assertEquals(1, partitionResolver.partitionIdFor("couchdb"));
        assertEquals(1, partitionResolver.partitionIdFor("Couchdb"));
        assertEquals(0, partitionResolver.partitionIdFor("CouchDB"));
        assertEquals(2, partitionResolver.partitionIdFor("Ektorp"));
        assertEquals(0, partitionResolver.partitionIdFor("Ektorp-cluster"));

    }

    @Test
    public void partition_of_10_node_should_follow_lounge_algorithm(){
        PartitionResolver partitionResolver = new PartitionResolver(10);

        assertEquals(0, partitionResolver.partitionIdFor(""));
        assertEquals(5, partitionResolver.partitionIdFor("CouchDB"));

        assertEquals(0, partitionResolver.partitionIdFor("Ektorp-cluster-0"));
        assertEquals(1, partitionResolver.partitionIdFor("Ektorp-cluster-1"));
        assertEquals(4, partitionResolver.partitionIdFor("Ektorp-cluster-2"));
        assertEquals(5, partitionResolver.partitionIdFor("Ektorp-cluster-3"));
        assertEquals(9, partitionResolver.partitionIdFor("Ektorp-cluster-4"));
        assertEquals(0, partitionResolver.partitionIdFor("Ektorp-cluster-5"));
        assertEquals(1, partitionResolver.partitionIdFor("Ektorp-cluster-6"));
        assertEquals(4, partitionResolver.partitionIdFor("Ektorp-cluster-7"));
        assertEquals(9, partitionResolver.partitionIdFor("Ektorp-cluster-8"));
        assertEquals(8, partitionResolver.partitionIdFor("Ektorp-cluster-9"));

    }

}
