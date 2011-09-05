package org.ektorp.impl.cluster;


import org.apache.http.conn.HttpHostConnectException;
import org.ektorp.DbAccessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ConnectException;

public class CouchMethodRepeater {

    private final static Logger logger = LoggerFactory.getLogger(CouchMethodRepeater.class);

    private final NodeSet nodeSet;

    public CouchMethodRepeater(NodeSet nodeSet) {
        this.nodeSet = nodeSet;
    }

    public Object repeat(String docId, CouchMethodRunner runner) {
        while (true) {
            Node node = nodeSet.nodeForId(docId);
            try {
                return runner.run(node.connector());
            }
            catch (DbAccessException e) {
                if (e.getCause() instanceof IOException){
                    logger.warn("Node has been marked unavailable: "+node, e);
                    node.markFailed();
                } else {
                    throw e;
                }
            }
            catch (ShardUnavailableException sue){
                throw sue;
            }

        }
    }

}
