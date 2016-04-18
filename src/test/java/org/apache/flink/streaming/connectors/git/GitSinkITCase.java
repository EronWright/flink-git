package org.apache.flink.streaming.connectors.git;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.eclipse.jgit.lib.*;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

/**
 * Tests the GitSink.
 */
public class GitSinkITCase extends StreamingMultipleProgramsTestBase {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Test the checkpoint behavior of the GitSink.
     */
    @Test
    public void testCheckpointing1() throws Exception {
        final int PARALLELISM = 1;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(1000);

        DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(2)).broadcast();

        File outPath = tempFolder.newFolder("out");
        source.addSink(new GitSink<Tuple2<Integer, String>>(new Path(outPath.toURI()).toString()));

        JobExecutionResult result = env.execute("testCheckpointing1");
        result.getJobID();

        // verify the contents of the repository.
        Repository repository = new RepositoryBuilder().setGitDir(new File(outPath, "git-0")).setMustExist(true).setBare().build();
        Ref head = repository.exactRef("refs/heads/master");
        try (RevWalk walk = new RevWalk(repository)) {
            RevCommit checkpointOnClose = walk.parseCommit(head.getObjectId());
            Assert.assertNotNull(checkpointOnClose);
            walk.markStart(checkpointOnClose);
            walk.next();
            RevCommit checkpoint2 = walk.next();
            Assert.assertNotNull(checkpoint2);
            RevCommit checkpoint1 = walk.next();
            Assert.assertNotNull(checkpoint1);
            Assert.assertEquals(0, checkpoint1.getParents().length);
        }
    }

    /**
     * Emit records until exactly numCheckpoints occur.
     *
     * Checkpoints are based on stream barriers that flow with the records;
     * by ceasing to emit records after checkpoint X, the sink is guaranteed to
     * not receive any records after checkpoint X.  This arrangement stabilizes the test.
     */
    private static class TestSourceFunction implements SourceFunction<Tuple2<Integer, String>>, Checkpointed<Long> {
        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        private int numCheckpoints;

        public TestSourceFunction(int numCheckpoints) {
            this.numCheckpoints = numCheckpoints;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
            int i = 0;
            while(running) {
                synchronized (ctx.getCheckpointLock()) {
                    if(running) {
                        ctx.collect(Tuple2.of(i, String.format("message #%d\n", i)));
                        i++;
                        Thread.sleep(10);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
            if(--numCheckpoints == 0) {
                running = false;
            }
            return 0L;
        }

        @Override
        public void restoreState(Long state) throws Exception {
        }
    }
}

