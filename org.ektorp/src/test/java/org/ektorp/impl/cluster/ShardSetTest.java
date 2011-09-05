package org.ektorp.impl.cluster;


import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

public class ShardSetTest {

    private final PartitionResolver mockPartitionResolver = mock(PartitionResolver.class);

    private final Shard shard1 = mock(Shard.class);
    private final Shard shard2 = mock(Shard.class);
    private final Shard shard3 = mock(Shard.class);
    private final Shard shard4 = mock(Shard.class);

    private ShardSet shardSet;

    @Before
    public void setUp() throws Exception {
        shardSet = new ShardSet(mockPartitionResolver, shard1, shard2, shard3, shard4);
    }

    @Test
    public void shard_is_picked_from_partitioner(){
        when(mockPartitionResolver.partitionIdFor("docid")).thenReturn(1);
        assertEquals(shard2, shardSet.shardForId("docid"));
    }

}
