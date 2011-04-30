package org.menagerie.locks;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.util.TestingThreadFactory;
import org.menagerie.util.TimingAccumulator;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * @author Scott Fines
 *         Date: Apr 26, 2011
 *         Time: 8:49:12 AM
 */
@Ignore
public class ReentrantZkLock2LoadTest {

    private static final String hostString = "localhost:2181";
    private static final String baseLockPath = "/test-locks";
    private static final int timeout = 2000;


    private static ZooKeeper zk;
    private static ZkSessionManager zkSessionManager;

    private static final int concurrency = 10;
    private static final int MAX_LOCK_ATTEMPTS = 1000;


    private final ExecutorService testService = Executors.newFixedThreadPool(concurrency,new TestingThreadFactory());

    @Before
    public void setup() throws Exception {

        zk = newZooKeeper();

        //be sure that the lock-place is created

        zk.create(baseLockPath,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zkSessionManager = new BaseZkSessionManager(zk);
    }

    @After
    public void tearDown() throws Exception{
        try{
            List<String> children = zk.getChildren(baseLockPath,false);
            for(String child:children){
                ZkUtils.safeDelete(zk,baseLockPath+"/"+child,-1);
            }
            ZkUtils.safeDelete(zk,baseLockPath,-1);

        }catch(KeeperException ke){
            //suppress because who cares what went wrong after our tests did their thing?
        }finally{
            zk.close();
        }
    }

    @Test//(timeout = 10000)
    public void loadTestLockWithDifferentLockInstances() throws Exception{
        /*
        The point of this test is to help increase confidence in the lock by showing that it
        does not deadlock under contention from tens to hundreds of threads.

        To have this kind of confidence, we have an unsafe operator, which we call
        in sequence after acquiring the lock. We can verify that it was correct by
         at the end checking that our total is what it should be
        */

        final UnsafeOperator operator = new UnsafeOperator();
        final CyclicBarrier startLatch = new CyclicBarrier(concurrency+1);
        final CyclicBarrier endLatch = new CyclicBarrier(concurrency+1);
        final TimingAccumulator timer = new TimingAccumulator();
        for(int i=0;i<concurrency;i++){
            testService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    } catch (BrokenBarrierException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    }
                    ZooKeeper zk = newZooKeeper();
                    try{
                        ZkSessionManager zksm = new BaseZkSessionManager(zk);
                        Lock guard = new ReentrantZkLock2(baseLockPath,zksm);
                        long totalStart = System.currentTimeMillis();
                        for(int i=0;i<MAX_LOCK_ATTEMPTS;i++){
                            timer.addTiming(guardedWork(guard,operator));
                        }
                        long totalEnd = System.currentTimeMillis();
                        long totalTime = totalEnd-totalStart;
                        float totalTimeSec = totalTime/1000f;
                        int ops = MAX_LOCK_ATTEMPTS;
                        float opsPerSec = ops/totalTimeSec;
                        System.out.printf("%d - total time taken: %d ms \t total Operations performed: %d \t Ops/sec: %f%n",Thread.currentThread().getId(),totalTime,ops,opsPerSec);
                    }finally{
                        zk.close();
                    }

                    //enter the stop latch
                    try {
                        endLatch.await();
                    } catch (InterruptedException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    } catch (BrokenBarrierException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    }
                    return null;
                }
            });
        }

        //start the test
        startLatch.await();

        //enter the end latch to wait on things finishing
        endLatch.await();

        int numberOfOperations = MAX_LOCK_ATTEMPTS*concurrency;
        assertEquals("Incorrect incrementing!",numberOfOperations,operator.getValue());
        timer.printResults();

    }

    @Test//(timeout = 10000)
    public void loadTestLockWithDifferentLockInstancesSameZooKeeper() throws Exception{
        /*
        The point of this test is to help increase confidence in the lock by showing that it
        does not deadlock under contention from tens to hundreds of threads.

        To have this kind of confidence, we have an unsafe operator, which we call
        in sequence after acquiring the lock. We can verify that it was correct by
         at the end checking that our total is what it should be
        */
        final UnsafeOperator operator = new UnsafeOperator();
        final CyclicBarrier startLatch = new CyclicBarrier(concurrency+1);
        final CyclicBarrier endLatch = new CyclicBarrier(concurrency+1);
        final TimingAccumulator timer = new TimingAccumulator();
        for(int i=0;i<concurrency;i++){
            testService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    } catch (BrokenBarrierException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    }
                    Lock guard = new ReentrantZkLock2(baseLockPath,zkSessionManager);
                    long totalStart = System.currentTimeMillis();
                    for(int i=0;i<MAX_LOCK_ATTEMPTS;i++){
                        timer.addTiming(guardedWork(guard,operator));
                    }
                    long totalEnd = System.currentTimeMillis();
                    long totalTime = totalEnd-totalStart;
                    float totalTimeSec = totalTime/1000f;
                    float opsPerSec = MAX_LOCK_ATTEMPTS/totalTimeSec;
                    System.out.printf("%d - total time taken: %d ms \t total Operations performed: %d \t Ops/sec: %f%n",Thread.currentThread().getId(),totalTime,MAX_LOCK_ATTEMPTS,opsPerSec);


                    //enter the stop latch
                    try {
                        endLatch.await();
                    } catch (InterruptedException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    } catch (BrokenBarrierException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    }
                    return null;
                }
            });
        }

        //start the test
        startLatch.await();

        //enter the end latch to wait on things finishing
        endLatch.await();

        int numberOfOperations = MAX_LOCK_ATTEMPTS*concurrency;
        assertEquals("Incorrect incrementing!",numberOfOperations,operator.getValue());
        timer.printResults();

    }


    @Test//(timeout = 10000)
    public void loadTestLockWithSameLockInstance() throws Exception{
        /*
        The point of this test is to help increase confidence in the lock by showing that it
        does not deadlock under contention from tens to hundreds of threads.

        To have this kind of confidence, we have an unsafe operator, which we call
        in sequence after acquiring the lock. We can verify that it was correct by
         at the end checking that our total is what it should be
        */

        final UnsafeOperator operator = new UnsafeOperator();
        final CyclicBarrier startLatch = new CyclicBarrier(concurrency+1);
        final CyclicBarrier endLatch = new CyclicBarrier(concurrency+1);
        final Lock guard = new ReentrantZkLock2(baseLockPath,zkSessionManager);
        final TimingAccumulator timer = new TimingAccumulator();
        for(int i=0;i<concurrency;i++){
            testService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    //enter the start latch
                    try {
                        startLatch.await();
                    } catch (InterruptedException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    } catch (BrokenBarrierException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    }

                    long totalStart = System.currentTimeMillis();
                    for(int i=0;i<MAX_LOCK_ATTEMPTS;i++){
                        timer.addTiming(guardedWork(guard,operator));
                    }
                    long totalEnd = System.currentTimeMillis();

                    long totalTime = totalEnd-totalStart;
                    float totalTimeSec = totalTime/1000f;
                    float opsPerSec = MAX_LOCK_ATTEMPTS/totalTimeSec;
                    System.out.printf("%d - total time taken: %d ms \t total Operations performed: %d \t Ops/sec: %f%n",Thread.currentThread().getId(),totalTime,MAX_LOCK_ATTEMPTS,opsPerSec);


                    //enter the stop latch
                    try {
                        endLatch.await();
                    } catch (InterruptedException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    } catch (BrokenBarrierException e) {
                        fail(String.format("%d - %s",Thread.currentThread().getId(),e.getMessage()));
                    }
                    return null;
                }
            });
        }

        //start the test
        startLatch.await();

        //enter the end latch to wait on things finishing
        endLatch.await();

        int numberOfOperations = MAX_LOCK_ATTEMPTS*concurrency;
        assertEquals("Incorrect incrementing!",numberOfOperations,operator.getValue());
        timer.printResults();
    }


    private long guardedWork(Lock guard, UnsafeOperator operator) {
        long start = System.currentTimeMillis();
        guard.lock();
        try{
            operator.increment();
        }finally{
            guard.unlock();
        }
        long end = System.currentTimeMillis();
        return end - start;
    }

    private static ZooKeeper newZooKeeper() throws IOException {
        return new ZooKeeper(hostString, timeout,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
//                System.out.println(event);
            }
        });
    }

}
