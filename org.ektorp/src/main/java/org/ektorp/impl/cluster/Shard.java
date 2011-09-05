package org.ektorp.impl.cluster;


import org.ektorp.*;
import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.ChangesFeed;
import org.ektorp.changes.DocumentChange;
import org.ektorp.http.HttpClient;
import org.ektorp.util.Documents;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class Shard implements CouchDbConnector{

    private final NodeSet nodeSet;
    private final CouchMethodRepeater methodRepeater;

    public Shard(Node... nodes) {
        this.nodeSet = new NodeSet(nodes);
        this.methodRepeater = new CouchMethodRepeater(nodeSet);
    }

    public void monitorShard() {
        nodeSet.monitorNodes();
    }


    @Override
    public void create(final String id, final Object o) {
        methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                connector.create(id, o);
                return null;
            }
        });

    }

    @Override
    public void create(final Object o) {
        final String id = Documents.getId(o);
        methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                connector.create(o);
                return null;
            }
        });
    }

    @Override
    public void update(final Object o) {
        final String id = Documents.getId(o);
        methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                connector.update(o);
                return null;
            }
        });
    }

    @Override
    public String delete(final Object o) {
        final String id = Documents.getId(o);
        return (String) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.delete(o);
            }
        });
    }

    @Override
    public String delete(final String id, final String revision) {
        return (String) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.delete(id, revision);
            }
        });
    }

    @Override
    public PurgeResult purge(Map<String, List<String>> revisionsToPurge) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(final Class<T> c, final String id) {
        return (T) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.get(c, id);
            }
        });
    }

    @Override
    public <T> T get(final Class<T> c, final String id, final Options options) {
        return (T) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.get(c, id, options);
            }
        });
    }

    @Override
    public <T> T find(final Class<T> c, final String id) {
        return (T) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.find(c, id);
            }
        });
    }

    @Override
    public <T> T find(final Class<T> c, final String id, final Options options) {
        return (T) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.find(c, id, options);
            }
        });
    }

    @Override
    @Deprecated
    public <T> T get(final Class<T> c, final String id, final String rev) {
        return (T) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.get(c, id, rev);
            }
        });
    }

    @Override
    public <T> T getWithConflicts(final Class<T> c, final String id) {
        return (T) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.getWithConflicts(c, id);
            }
        });
    }

    @Override
    public boolean contains(final String id) {
        return (Boolean) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.contains(id);
            }
        });
    }

    @Override
    public InputStream getAsStream(final String id) {
        return (InputStream) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.getAsStream(id);
            }
        });
    }

    @Override
    @Deprecated
    public InputStream getAsStream(final String id, final String rev) {
        return (InputStream) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.getAsStream(id, rev);
            }
        });
    }

    @Override
    public InputStream getAsStream(final String id, final Options options) {
        return (InputStream) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.getAsStream(id, options);
            }
        });
    }

    @Override
    public List<Revision> getRevisions(final String id) {
        return (List<Revision>) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.getRevisions(id);
            }
        });
    }

    @Override
    public AttachmentInputStream getAttachment(final String id, final String attachmentId) {
        return (AttachmentInputStream) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.getAttachment(id, attachmentId);
            }
        });
    }

    @Override
    public String createAttachment(final String id, final AttachmentInputStream data) {
        return (String) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.createAttachment(id, data);
            }
        });
    }

    @Override
    public String createAttachment(final String id, final String revision, final AttachmentInputStream data) {
        return (String) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.createAttachment(id, revision, data);
            }
        });
    }

    @Override
    public String deleteAttachment(final String id, final String revision, final String attachmentId) {
        return (String) methodRepeater.repeat(id, new CouchMethodRunner() {
            public Object run(CouchDbConnector connector) {
                return connector.deleteAttachment(id, revision, attachmentId);
            }
        });
    }

    @Override
    public List<String> getAllDocIds() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> List<T> queryView(ViewQuery query, Class<T> type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Page<T> queryForPage(ViewQuery query, PageRequest pr, Class<T> type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ViewResult queryView(ViewQuery query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public StreamingViewResult queryForStreamingView(ViewQuery query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public InputStream queryForStream(ViewQuery query) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createDatabaseIfNotExists() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDatabaseName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String path() {
        throw new UnsupportedOperationException();
    }

    @Override
    public HttpClient getConnection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DbInfo getDbInfo() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DesignDocInfo getDesignDocInfo(String designDocId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compact() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void compactViews(String designDocumentId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanupViews() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRevisionLimit() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setRevisionLimit(int limit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReplicationStatus replicateFrom(String source) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReplicationStatus replicateFrom(String source, Collection<String> docIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReplicationStatus replicateTo(String target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ReplicationStatus replicateTo(String target, Collection<String> docIds) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addToBulkBuffer(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DocumentOperationResult> flushBulkBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearBulkBuffer() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DocumentOperationResult> executeBulk(InputStream inputStream) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DocumentOperationResult> executeAllOrNothing(InputStream inputStream) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DocumentOperationResult> executeBulk(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DocumentOperationResult> executeAllOrNothing(Collection<?> objects) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<DocumentChange> changes(ChangesCommand cmd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ChangesFeed changesFeed(ChangesCommand cmd) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String callUpdateHandler(String designDocID, String function, String docId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String callUpdateHandler(String designDocID, String function, String docId, Map<String, String> params) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T callUpdateHandler(UpdateHandlerRequest req, Class<T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String callUpdateHandler(UpdateHandlerRequest req) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureFullCommit() {
        throw new UnsupportedOperationException();
    }

}
