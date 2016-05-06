package org.apache.flink.streaming.connectors.git;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.eclipse.jgit.lib.*;

import java.io.File;

/**
 * A log writer based on low-level git concepts.
 *
 * Not thread-safe.
 *
 * http://www.codeaffine.com/2014/10/20/git-internals/
 */
public class LogWriter {

    private final RuntimeContext context;

    private final Repository repository;

    private ObjectId currentCommitId;

    /**
     * The inserter with which to insert objects into the repository.
     */
    private ObjectInserter inserter;

    /**
     * The tree into which objects are currently being written.   At checkpoint time,
     * the tree is committed.
     */
    private TreeFormatter tree;

    private long sequenceNumber;

    public LogWriter(Repository repository, ObjectId initialCommitId, RuntimeContext context) {
        this.context = context;
        this.repository = repository;
        this.currentCommitId = initialCommitId;
    }

    public void nextTree() throws Exception {
        this.tree = new TreeFormatter();

        // reset the sequence number; each commit will replace the previous files (but retain the underlying objects).
        this.sequenceNumber = 0;
    }

    public void open() throws Exception {
        this.inserter = repository.newObjectInserter();
        nextTree();
    }

    public void close() throws Exception {
        inserter.close();
    }

    public void write(byte[] data) throws Exception {
        ObjectId blobId = inserter.insert(Constants.OBJ_BLOB, data);
        tree.append(sequenceNumberToName(sequenceNumber++), FileMode.REGULAR_FILE, blobId);
    }

    public ObjectId checkpoint(long checkpointId) throws Exception {

        // insert the current tree into the repository, and initialize a new tree
        ObjectId treeId = inserter.insert(tree);
        nextTree();

        // insert a commit referring to the tree
        PersonIdent committer = new PersonIdent(context.getTaskNameWithSubtasks(), "");
        CommitBuilder commit = new CommitBuilder();
        if(!currentCommitId.equals(ObjectId.zeroId())) commit.addParentId(currentCommitId);
        commit.setTreeId(treeId);
        commit.setAuthor(committer);
        commit.setCommitter(committer);
        commit.setMessage(String.format("checkpointId: %d", checkpointId));
        ObjectId commitId = inserter.insert(commit);
        inserter.flush();

        // update this log's reference to point to the latest commit
        String branchName = "partition-" + (context.getIndexOfThisSubtask() + 1);
        RefUpdate refUpdate = repository.getRefDatabase().newUpdate(Constants.R_HEADS + branchName, false);
        refUpdate.setExpectedOldObjectId(currentCommitId);
        refUpdate.setNewObjectId(commitId);
        refUpdate.setRefLogMessage(String.format("checkpointId: %d", checkpointId), false);
        RefUpdate.Result result = refUpdate.update();

        if(result != RefUpdate.Result.NEW && result != RefUpdate.Result.FAST_FORWARD) {
            throw new RuntimeException("unexpected outcome: " + result);
        }

        currentCommitId = commitId;
        return commitId;
    }

    /**
     * Convert a sequence number to a filename.
     *
     * The entries of a git tree are ordered by the byte sequence comprising their name.
     */
    private static byte[] sequenceNumberToName(long sequenceNumber) {
        return String.format("%019d", sequenceNumber).getBytes();
    }
}
