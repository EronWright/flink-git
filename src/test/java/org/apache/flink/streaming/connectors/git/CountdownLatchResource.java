package org.apache.flink.streaming.connectors.git;

import org.junit.rules.ExternalResource;

import java.util.concurrent.CountDownLatch;

/**
 * A test resource providing a countdown latch.
 */
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
