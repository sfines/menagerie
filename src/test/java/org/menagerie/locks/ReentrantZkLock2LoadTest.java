package org.menagerie.locks;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;
import org.menagerie.TimingAccumulator;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;

import static org.junit.Assert.fail;

/**
 * @author Scott Fines
 *         Date: Apr 26, 2011
 *         Time: 8:49:12 AM
 */
public class ReentrantZkLock2LoadTest {

    private static final String hostString = "localhost:2181";
    private static final String baseLockPath = "/test-locks";
    private static final int timeout = 2000;


    private static ZooKeeper zk;
    private static ZkSessionManager zkSessionManager;

    private static final int concurrency = 10;
    private static final int MAX_LOCK_ATTEMPTS = 1000;
    private static final float insertThreshold =0.75f;

    private final ExecutorService testService = Executors.newFixedThreadPool(concurrency);
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

        To have this kind of confidence, we have an unsynchronized map, which we use a distributed
        Lock to guard against. Each Thread then will acquire the lock, then with a probability of 0.75
        will add an entry, and with probability of 0.25 will delete a random entry, then it will release
        the lock, allowing the next node access. Each thread will do this MAX_LOCK_ATTEMPTS times, then
        exit.
        */

        final Map<Integer,Integer> guardedMap = new HashMap<Integer, Integer>();
        final Random random = new Random();
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
                    ZkSessionManager zksm = new BaseZkSessionManager(zk);
                    Lock guard = new ReentrantZkLock2(baseLockPath,zksm);
                    long totalStart = System.currentTimeMillis();
                    for(int i=0;i<MAX_LOCK_ATTEMPTS;i++){
                        timer.addTiming(guardedWork(guard,random,insertThreshold,guardedMap));
                    }
                    long totalEnd = System.currentTimeMillis();
                    long totalTime = totalEnd-totalStart;
                    float totalTimeSec = totalTime/1000f;
                    int ops = MAX_LOCK_ATTEMPTS;
                    float opsPerSec = ops/totalTimeSec;
                    System.out.printf("%d - total time taken: %d ms \t total Operations performed: %d \t Ops/sec: %f%n",Thread.currentThread().getId(),totalTime,ops,opsPerSec);

                    
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

        timer.printResults();

    }

    @Test//(timeout = 10000)
    public void loadTestLockWithDifferentLockInstancesSameZooKeeper() throws Exception{
        /*
        The point of this test is to help increase confidence in the lock by showing that it
        does not deadlock under contention from tens to hundreds of threads.

        To have this kind of confidence, we have an unsynchronized map, which we use a distributed
        Lock to guard against. Each Thread then will acquire the lock, then with a probability of 0.75
        will add an entry, and with probability of 0.25 will delete a random entry, then it will release
        the lock, allowing the next node access. Each thread will do this MAX_LOCK_ATTEMPTS times, then
        exit.
        */

        final Map<Integer,Integer> guardedMap = new HashMap<Integer, Integer>();
        final Random random = new Random();
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
                        timer.addTiming(guardedWork(guard,random,insertThreshold,guardedMap));
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

        timer.printResults();

    }


    @Test//(timeout = 10000)
    public void loadTestLockWithSameLockInstance() throws Exception{
        /*
        The point of this test is to help increase confidence in the lock by showing that it
        does not deadlock under contention from tens to hundreds of threads.

        To have this kind of confidence, we have an unsynchronized map, which we use a distributed
        Lock to guard against. Each Thread then will acquire the lock, then with a probability of 0.75
        will add an entry, and with probability of 0.25 will delete a random entry, then it will release
        the lock, allowing the next node access. Each thread will do this MAX_LOCK_ATTEMPTS times, then
        exit.
        */

        final Map<Integer,Integer> guardedMap = new HashMap<Integer, Integer>();
        final Random random = new Random();
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
                        timer.addTiming(guardedWork(guard,random,insertThreshold,guardedMap));
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

        timer.printResults();
    }


    private long guardedWork(Lock guard, Random random, float insertThreshold, Map<Integer, Integer> guardedMap) {
        long start = System.currentTimeMillis();
        guard.lock();
        try{
            float shouldInsertOrDelete = random.nextFloat();
            if(shouldInsertOrDelete<insertThreshold){
                guardedMap.put(random.nextInt(),1);
            }else{
                guardedMap.remove(random.nextInt());
            }
        }finally{
            guard.unlock();
        }
        long end = System.currentTimeMillis();
        long timing = end - start;
        return timing;
    }

    private static ZooKeeper newZooKeeper() throws IOException {
        return new ZooKeeper(hostString, timeout,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
    }
}
