package org.menagerie.latches;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Dec-2010
 *          Time: 13:34:25
 */
public class ZkCountDownLatchTest {
    private static ZooKeeper zk;
    private static final String baseBarrierPath = "/test-barriers";
    private static final int timeout = 200000;
    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    private ZkCountDownLatch countDownLatch;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {

        zk = newZooKeeper();

        //be sure that the lock-place is created
        try{
            zk.create(baseBarrierPath,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException ke){
            if(ke.code()!= KeeperException.Code.NODEEXISTS)
                throw ke;
        }
    }



    @AfterClass
    public static void tearDownAfterClass() throws Exception{
        try{
            List<String> children = zk.getChildren(baseBarrierPath,false);
            for(String child:children){
                zk.delete(baseBarrierPath +"/"+child,-1);
            }
            zk.delete(baseBarrierPath,-1);

        }catch(KeeperException ke){
            if(ke.code()!= KeeperException.Code.NONODE)
                throw ke;
        }finally{
            zk.close();
        }
    }

    @After
    public void tearDown() throws Exception{
        if(countDownLatch!=null)
            countDownLatch.closeLatch();
    }
    
    @Test(timeout = 5000l)
    public void testCountDownWorksWhenAwaitHappensAfterCountDown() throws Exception {
        countDownLatch = ZkLatches.newCountDownLatch(1, baseBarrierPath, new BaseZkSessionManager(zk));
        System.out.println(countDownLatch.getCount());
        countDownLatch.countDown();
        countDownLatch.await();
    }

    @Test(timeout = 5000l)
    public void testCountDownWorksWhenCountHappensOnAnotherThread() throws Exception{
        countDownLatch = ZkLatches.newCountDownLatch(1, baseBarrierPath, new BaseZkSessionManager(zk));
        executor.submit(new Runnable() {
            @Override
            public void run() {
                System.out.printf("Counting down at :%s%n",System.currentTimeMillis());
                countDownLatch.countDown();
            }
        });
        System.out.printf("Waiting for count down at :%s--Thread Id=%s,%n",System.currentTimeMillis(),Thread.currentThread().getName());
        countDownLatch.await();
        System.out.printf("Count Down occurred. Time: %s%n",System.currentTimeMillis());
    }

    @Test(timeout = 5000l)
    public void testCountDownWorksWithTwoCountsOnOtherThreads() throws Exception{
        countDownLatch = ZkLatches.newCountDownLatch(2, baseBarrierPath, new BaseZkSessionManager(zk));
        executor.submit(new Runnable() {
            @Override
            public void run() {
                System.out.printf("#1 Counting down at :%s%n",System.currentTimeMillis());
                countDownLatch.countDown();
            }
        });

        executor.submit(new Runnable() {
            @Override
            public void run() {
                System.out.printf("#2 Counting down at :%s%n",System.currentTimeMillis());
                countDownLatch.countDown();
            }
        });

        System.out.printf("Waiting for count down at :%s--Thread Id=%s,%n",System.currentTimeMillis(),Thread.currentThread().getName());
        countDownLatch.await();
        System.out.printf("Count Down occurred. Time: %s%n",System.currentTimeMillis());
    }

    @Test(timeout = 5000l)
    public void testCountDownWorksWithTwoClients() throws Exception{
         countDownLatch = ZkLatches.newCountDownLatch(2, baseBarrierPath, new BaseZkSessionManager(zk));
        final ZooKeeper myOtherZooKeeper = newZooKeeper();
        final ZkCountDownLatch joinedCountDownLatch = ZkLatches.newCountDownLatch(2,baseBarrierPath,new BaseZkSessionManager(myOtherZooKeeper));

        executor.submit(new Runnable() {
            @Override
            public void run() {
                System.out.printf("#1 Counting down at :%s%n",System.currentTimeMillis());
                countDownLatch.countDown();
            }
        });

        executor.submit(new Runnable() {
            @Override
            public void run() {
                System.out.printf("#2 Counting down at :%s%n",System.currentTimeMillis());
                joinedCountDownLatch.countDown();
            }
        });
        try{
            System.out.printf("Waiting for count down at :%s--Thread Id=%s,%n",System.currentTimeMillis(),Thread.currentThread().getName());
            countDownLatch.await();
            System.out.printf("Count Down occurred. Time: %s%n",System.currentTimeMillis());
        }finally{
            joinedCountDownLatch.closeLatch();
        }
    }

    @Test(timeout = 5000l)
    public void testCountDownSucceedsWithOneClientFailedAfterCountingDown() throws Exception{
        countDownLatch = ZkLatches.newCountDownLatch(2, baseBarrierPath, new BaseZkSessionManager(zk));
        final ZooKeeper myOtherZooKeeper = newZooKeeper();
        final ZkCountDownLatch joinedCountDownLatch = ZkLatches.newCountDownLatch(2,baseBarrierPath,new BaseZkSessionManager(myOtherZooKeeper));
        System.out.printf("#1 Counting down at :%s%n",System.currentTimeMillis());
        joinedCountDownLatch.countDown();
        try {
            System.out.printf("Closing my zk client%n");
            myOtherZooKeeper.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.printf("#2 Counting down at :%s%n",System.currentTimeMillis());
        countDownLatch.countDown();

        System.out.printf("Waiting for count down at :%s--Thread Id=%s,%n",System.currentTimeMillis(),Thread.currentThread().getName());
        countDownLatch.await();
        System.out.printf("Count Down occurred. Time: %s%n",System.currentTimeMillis());
    }

    @Test(timeout = 5000l)
    public void testMultipleCountDownsWork() throws Exception{
        countDownLatch = ZkLatches.newCountDownLatch(1, baseBarrierPath, new BaseZkSessionManager(zk));
        assertEquals("The Initial count down latch does not have a count of zero!",0,countDownLatch.getCount());
        countDownLatch.countDown();
        assertEquals("The Initial count down latch does not have a count of one!",1,countDownLatch.getCount());
        countDownLatch.await();
        countDownLatch.closeLatch();

        countDownLatch = ZkLatches.newCountDownLatch(1, baseBarrierPath, new BaseZkSessionManager(zk));
        assertEquals("The Initial count down latch does not have a count of zero!",0,countDownLatch.getCount());
        countDownLatch.countDown();
        assertEquals("The Initial count down latch does not have a count of one!",1,countDownLatch.getCount());
        countDownLatch.await();
    }

    @Test
    public void testLatchTimesOutCorrectly() throws Exception {
        countDownLatch = ZkLatches.newCountDownLatch(1, baseBarrierPath, new BaseZkSessionManager(zk));

        boolean alerted = countDownLatch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("The Count down latch returned an incorrect state!",!alerted);
    }

    private static ZooKeeper newZooKeeper() throws IOException {
        return new ZooKeeper("localhost:2181", timeout,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
    }
}
