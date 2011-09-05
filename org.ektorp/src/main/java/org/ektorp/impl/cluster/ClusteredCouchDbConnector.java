package org.ektorp.impl.cluster;


import org.ektorp.*;
import org.ektorp.changes.ChangesCommand;
import org.ektorp.changes.ChangesFeed;
import org.ektorp.changes.DocumentChange;
import org.ektorp.http.HttpClient;
import org.ektorp.util.Assert;
import org.ektorp.util.Documents;

import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ClusteredCouchDbConnector implements CouchDbConnector{

    private final ShardSet shardSet;
    private final ClusterMonitor clusterMonitor;

    public ClusteredCouchDbConnector(Shard... shards) {
        this.shardSet = new ShardSet(shards);
        clusterMonitor = new ClusterMonitor(shardSet);
        clusterMonitor.start();
    }

    public void stopClusterMonitor(){
        clusterMonitor.stop();
    }


    @Override
    public void create(String id, Object o) {
        shardSet.shardForId(id).create(id, o);
    }

    @Override
    public void create(Object o) {
        final String id = Documents.getId(o);
        // TODO: use /_uuids?count=1 to retrieve an id
        Assert.notNull(id, "Document must have an id provided for the moment");

        shardSet.shardForId(id).create(o);
    }

    @Override
    public void update(Object o) {
        final String id = Documents.getId(o);
        shardSet.shardForId(id).update(o);
    }

    @Override
    public String delete(Object o) {
        final String id = Documents.getId(o);
        return shardSet.shardForId(id).delete(o);
    }

    @Override
    public String delete(String id, String revision) {
        return shardSet.shardForId(id).delete(id, revision);
    }

    @Override
    public PurgeResult purge(Map<String, List<String>> revisionsToPurge) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T get(Class<T> c, String id) {
        return shardSet.shardForId(id).get(c, id);
    }

    @Override
    public <T> T get(Class<T> c, String id, Options options) {
        return shardSet.shardForId(id).get(c, id, options);
    }

    @Override
    public <T> T find(Class<T> c, String id) {
        return shardSet.shardForId(id).find(c, id);
    }

    @Override
    public <T> T find(Class<T> c, String id, Options options) {
        return shardSet.shardForId(id).find(c, id, options);
    }

    @Override
    public <T> T get(Class<T> c, String id, String rev) {
        return shardSet.shardForId(id).get(c, id, rev);
    }

    @Override
    public <T> T getWithConflicts(Class<T> c, String id) {
        return shardSet.shardForId(id).getWithConflicts(c, id);
    }

    @Override
    public boolean contains(String id) {
        return shardSet.shardForId(id).contains(id);
    }

    @Override
    public InputStream getAsStream(String id) {
        return shardSet.shardForId(id).getAsStream(id);
    }

    @Override
    public InputStream getAsStream(String id, String rev) {
        return shardSet.shardForId(id).getAsStream(id, rev);
    }

    @Override
    public InputStream getAsStream(String id, Options options) {
        return shardSet.shardForId(id).getAsStream(id, options);
    }

    @Override
    public List<Revision> getRevisions(String id) {
        return shardSet.shardForId(id).getRevisions(id);
    }

    @Override
    public AttachmentInputStream getAttachment(String id, String attachmentId) {
        return shardSet.shardForId(id).getAttachment(id, attachmentId);
    }

    @Override
    public String createAttachment(String id, AttachmentInputStream data) {
        return shardSet.shardForId(id).createAttachment(id, data);
    }

    @Override
    public String createAttachment(String id, String revision, AttachmentInputStream data) {
        return shardSet.shardForId(id).createAttachment(id, revision, data);
    }

    @Override
    public String deleteAttachment(String id, String revision, String attachmentId) {
        return shardSet.shardForId(id).deleteAttachment(id, revision, attachmentId);
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
