package org.ektorp.functionnal;


import org.ektorp.CouchDbConnector;
import org.ektorp.CouchDbInstance;
import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;
import org.ektorp.impl.StdCouchDbConnector;
import org.ektorp.impl.StdCouchDbInstance;
import org.ektorp.impl.cluster.ClusteredCouchDbConnector;
import org.ektorp.impl.cluster.Node;
import org.ektorp.impl.cluster.Shard;
import org.ektorp.impl.cluster.ShardSet;

import java.util.Random;

public class QuickTry {

    // TODO:
    // - generate id when missing
    // - implements missing connector implementation
    // - add bulk api

    public static void main(String[] args) {

        Node shardAnode1 = new Node(connectorForDatabase("shardAnode1"));
        Node shardAnode2 = new Node(connectorForDatabase("shardAnode2"));
        Shard shardA = new Shard(shardAnode1, shardAnode2);

        Node shardBnode1 = new Node(connectorForDatabase("shardBnode1"));
        Node shardBnode2 = new Node(connectorForDatabase("shardBnode2"));
        Shard shardB = new Shard(shardBnode1, shardBnode2);

        CouchDbConnector connector = new ClusteredCouchDbConnector(shardA, shardB);

        for (int i=0; i < 10000; i++){
            try {
                String id = "blue"+ new Random().nextInt(100000);
                Sofa sofa = new Sofa(id);
                sofa.setId(id);
                connector.create(sofa);
            } catch (Exception e){
                System.out.println("Ignoring "+e.getMessage());
            }
        }

    }

    private static CouchDbConnector connectorForDatabase(String dbName){
        HttpClient httpClient = new StdHttpClient.Builder().host("localhost").port(5984).build();

        CouchDbInstance dbInstance = new StdCouchDbInstance(httpClient);
        try {
            dbInstance.deleteDatabase(dbName);
        } catch (Exception ignore){}

        CouchDbConnector db = new StdCouchDbConnector(dbName, dbInstance);
        db.createDatabaseIfNotExists();
        return db;
    }
}
