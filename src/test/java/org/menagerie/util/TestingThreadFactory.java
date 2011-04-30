package org.menagerie.util;

import org.junit.Ignore;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: Apr 26, 2011
 *         Time: 6:54:31 PM
 */
@Ignore
public class TestingThreadFactory implements ThreadFactory {
    private final AtomicInteger counter = new AtomicInteger(0);
    @Override
    public Thread newThread(Runnable runnable) {
        Thread t = new Thread(runnable);
        t.setName("test-thread-"+counter.incrementAndGet());
        return t;
    }
}
