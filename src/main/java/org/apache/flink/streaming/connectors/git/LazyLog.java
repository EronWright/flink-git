package org.apache.flink.streaming.connectors.git;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.lib.Repository;

import java.io.File;

/**
 * A log implementation that commits at checkpoint time.
 */
public class LazyLog implements CheckpointListener {

    private final Repository repository;

    private final Git git;

    private DataFileWriter<Datum> writer;

    private final Schema schema = ReflectData.get().getSchema(Datum.class);

    public LazyLog(Repository repository) {
        this.repository = repository;
        this.git = new Git(repository);
    }

    public void open() throws Exception {
        LogFile logFile = new LogFile(new File(repository.getDirectory().getParent(), "log"));
    }

    public void close() throws Exception {

    }

    public void write(Datum value) throws Exception {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        git.commit()
                .setMessage(String.format("checkpointId: {}", checkpointId))
                .call();
    }

    /**
     * An append-only log file.
     */
    private class LogFile {
        private final File path;

        public LogFile(File path) {
            this.path = path;
        }

        public void open() throws Exception {


            DatumWriter<Datum> datumWriter = new ReflectDatumWriter<>(schema);
            writer = new DataFileWriter<>(datumWriter).appendTo(path).create(schema, path);

        }

        public void close() throws Exception {
            writer.close();
        }
    }

    static class Datum {
        public byte[] data;
    }
}
