package org.ektorp.impl.cluster;


import java.util.Arrays;
import java.util.List;

public class ShardSet {

    private final List<Shard> shards;
    private final PartitionResolver partitionResolver;

    public ShardSet(Shard... shards) {
        this(new PartitionResolver(shards.length), shards);
    }

    protected ShardSet(PartitionResolver partitionResolver, Shard... shards) {
        this.shards = Arrays.asList(shards);
        this.partitionResolver = partitionResolver;
    }

    public Shard shardForId(String id) {
        int shardId = partitionResolver.partitionIdFor(id);
        return shards.get(shardId);
    }

    public void monitorShards(){
        for (int i=0; i<shards.size(); i++){
            shards.get(i).monitorShard();
        }
    }
}
