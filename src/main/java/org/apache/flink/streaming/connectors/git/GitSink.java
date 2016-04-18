package org.apache.flink.streaming.connectors.git;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.PersonIdent;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.RepositoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

/**
 *
 */
public class GitSink<T> extends RichSinkFunction<T> implements InputTypeConfigurable, Checkpointed<GitSink.CheckpointState>, CheckpointListener {
    private static final long serialVersionUID = 1L;

    private static Logger LOG = LoggerFactory.getLogger(GitSink.class);

    /**
     * The base {@code Path} that stored all rolling bucket directories.
     */
    protected final String basePath;

    /**
     * Our subtask index, retrieved from the {@code RuntimeContext} in {@link #open}.
     */
    private transient int subtaskIndex;

    /**
     * The schema associated with the input type.
     */
    private TypeInformationSerializationSchema schema;

    /**
     * The repository handle.
     */
    protected transient Repository repository;

    /**
     * The open log writer.
     */
    private transient LogWriter currentWriter;

    public GitSink(String basePath) {
        this.basePath = basePath;
    }

    @Override
    public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
        schema = new TypeInformationSerializationSchema(type, executionConfig);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

        if(schema == null) throw new IllegalStateException("inputType must be set");

        // create or open the repository
        Path repoPath = new Path(basePath, "git" + "-" + subtaskIndex);
        if(!(repoPath.getFileSystem() instanceof LocalFileSystem)) {
            throw new UnsupportedOperationException("GitSink supports only the local filesystem");
        }
        File repoFile = pathToFile(repoPath);
        repository = new RepositoryBuilder().setGitDir(repoFile).setMustExist(false).setBare().build();
        repository.create(true);

        currentWriter = new LogWriter(repository, ObjectId.zeroId(), getRuntimeContext());
        currentWriter.open();
    }

    @Override
    public void close() throws Exception {

        // hack: issue a final checkpoint
        currentWriter.checkpoint(Long.MAX_VALUE);
        currentWriter.close();

        super.close();
    }


    @Override
    public void invoke(T value) throws Exception {
        byte[] data = schema.serialize(value);
        currentWriter.write(data);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
    }

    @Override
    public CheckpointState snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
        // commit written data
        ObjectId checkpointCommitId = currentWriter.checkpoint(checkpointId);
        return new CheckpointState(checkpointCommitId);
    }

    @Override
    public void restoreState(CheckpointState state) throws Exception {
        // abandon any data written since the most recent checkpoint.
        currentWriter.close();

        currentWriter = new LogWriter(repository, state.commitId, getRuntimeContext());
        currentWriter.open();
    }

    static class CheckpointState implements Serializable {
        private static final long serialVersionUID = 1L;

        /**
         * The commit id of the latest checkpoint.
         */
        public final ObjectId commitId;

        CheckpointState(ObjectId commitId) {
            this.commitId = commitId;
        }
    }

    private File pathToFile(Path path) throws IOException {
        if (!path.isAbsolute()) {
            path = new Path(path.getFileSystem().getWorkingDirectory(), path);
        }
        return new File(path.toUri().getPath());
    }
}

