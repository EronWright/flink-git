package org.apache.flink.streaming.connectors.git;

import org.junit.rules.ExternalResource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

/**
 * A test resource providing a cyclic barrier.
 */
class CyclicBarrierResource extends ExternalResource {

    private final int count;
    private CyclicBarrier barrier;

    public CyclicBarrierResource(int count) {
        this.count = count;
    }

    public CyclicBarrier barrier() {
        if(barrier == null) throw new IllegalStateException("must be called after before() and before after()");
        return this.barrier;
    }

    @Override
    protected void before() throws Throwable {
        super.before();
        this.barrier = new CyclicBarrier(count);
    }

    @Override
    protected void after() {
        super.after();
        this.barrier = null;
    }
}
