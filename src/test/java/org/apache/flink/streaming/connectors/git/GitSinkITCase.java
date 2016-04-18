package org.apache.flink.streaming.connectors.git;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by wrighe3 on 4/15/16.
 */
public class GitSinkITCase extends StreamingMultipleProgramsTestBase {
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    @ClassRule
    public static CountdownLatchResource latchHandle = new CountdownLatchResource(2);

    @Test
    public void testCheckpointing1() throws Exception {
        final int PARALLELISM = 1;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.enableCheckpointing(200);

        DataStream<Tuple2<Integer, String>> source = env.addSource(new GeneratorFunction() {
            @Override
            protected CountDownLatch latch() { return latchHandle.latch(); }
        }).broadcast();

        File outPath = tempFolder.newFolder("out");

        GitSink<Tuple2<Integer, String>> sink = new GitSink<Tuple2<Integer, String>>(new Path(outPath.toURI()).toString()) {
            @Override
            public void notifyCheckpointComplete(long checkpointId) throws Exception {
                latchHandle.latch().countDown();
            }
        };

        source.addSink(sink);

        JobExecutionResult result = env.execute("testCheckpointing1");
        result.getJobID();
    }

    private static abstract class GeneratorFunction implements SourceFunction<Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;

        protected abstract CountDownLatch latch();


        @Override
        public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
            int i = 0;

            do {
                ctx.collect(Tuple2.of(i, String.format("message #%d\n", i)));
                i++;
            }
            while(!latch().await(10, TimeUnit.MILLISECONDS));
        }

        @Override
        public void cancel() {
            latch().countDown();
        }
    }

    private static class TestSourceFunction implements SourceFunction<Tuple2<Integer, String>> {
        private static final long serialVersionUID = 1L;

        private volatile boolean running = true;

        private final int numElements;

        public TestSourceFunction(int numElements) {
            this.numElements = numElements;
        }

        @Override
        public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
            for (int i = 0; i < numElements && running; i++) {
                ctx.collect(Tuple2.of(i, "message #\n" + i));
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

class CountdownLatchResource extends ExternalResource {

    private final int count;
    private CountDownLatch latch;

    public CountdownLatchResource(int count) {
        this.count = count;
    }

    public CountDownLatch latch() {
        if(latch == null) throw new IllegalStateException("must be called after before() and before after()");
        return this.latch;
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        this.latch = new CountDownLatch(count);
    }

    @Override
    protected void after() {
        super.after();
        this.latch = null;
    }
}