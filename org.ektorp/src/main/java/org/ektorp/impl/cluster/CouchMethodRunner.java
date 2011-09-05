package org.ektorp.impl.cluster;


import org.ektorp.CouchDbConnector;

public interface CouchMethodRunner {

    public Object run(CouchDbConnector connector);

}
