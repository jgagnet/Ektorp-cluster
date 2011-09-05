package org.ektorp.impl.cluster;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterMonitor implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(ClusterMonitor.class);

    private final ShardSet shardSet;

    private Thread thread;

    public ClusterMonitor(ShardSet shardSet) {
        this.shardSet = shardSet;
    }

    public void start(){
        thread = new Thread(this);
        thread.start();
    }

    public void stop(){
        thread = null;
    }


    @Override
    public void run() {
        while (thread != null){
            try {
                shardSet.monitorShards();
            } catch (Exception e){
                logger.error("Exception in Cluster Monitor", e);
            }

            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException expected) {}
        }
    }
}
