package org.menagerie.latches;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.util.TestingThreadFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Jan-2011
 *          Time: 19:25:46
 */
public class ZkSemaphoreTest {

    private static final String hostString = "localhost:2181";
    private static final String basePath = "/test-semaphores";
    private static final int timeout = 2000;
    private static final ExecutorService testService = Executors.newFixedThreadPool(2, new TestingThreadFactory());

    private static ZooKeeper zk;
    private static ZkSessionManager zkSessionManager;

    @Before
    public void setup() throws Exception {

        zk = newZooKeeper();

        //be sure that the lock-place is created

        zk.create(basePath,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zkSessionManager = new BaseZkSessionManager(zk);
    }

    @After
    public void tearDown() throws Exception{
        try{
            List<String> children = zk.getChildren(basePath,false);
            for(String child:children){
                ZkUtils.safeDelete(zk, basePath +"/"+child,-1);
            }
            ZkUtils.safeDelete(zk, basePath,-1);

        }catch(KeeperException ke){
            //suppress because who cares what went wrong after our tests did their thing?
        }finally{
            zk.close();
        }
    }

    @Test(timeout = 1000l)
    public void testAcquireSucceedsOneThread() throws Exception{
        int numPermits = 1;
        ZkSemaphore semaphore = new ZkSemaphore(numPermits, basePath,zkSessionManager);

        semaphore.acquire();

        //check that the available permits are zero
        assertEquals("semaphore does not report fewer permits!",numPermits-1,semaphore.availablePermits());
        semaphore.release();
        assertEquals("semaphore does not report releasing a permit!",numPermits,semaphore.availablePermits());
    }

    @Test(timeout = 1000l)
    public void testTwoThreadsCanAccessSempahore() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final ZkSemaphore semaphore = new ZkSemaphore(2,basePath,zkSessionManager);

        testService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    semaphore.acquire();
                    latch.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        semaphore.acquire();
        latch.await();
    }

    @Test(timeout = 1000l)
    public void testTwoClientsCanAccessSempahore() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final ZkSemaphore semaphore = new ZkSemaphore(2,basePath,zkSessionManager);

        semaphore.acquire();
        Future<Void> errorFuture = testService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ZooKeeper zk = newZooKeeper();
                try {

                    ZkSemaphore semaphore2 = new ZkSemaphore(2, basePath, new BaseZkSessionManager(zk));
                    semaphore2.acquire();
                    latch.countDown();
                } finally {
                    zk.close();
                }
                return null;
            }
        });

        latch.await();
        errorFuture.get();
    }


    @Test(timeout = 1500l)
    public void testRunOutOfPermitsTwoThreads() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final ZkSemaphore semaphore = new ZkSemaphore(1,basePath,zkSessionManager);
        semaphore.acquire();
        Future<?> errorFuture = testService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    semaphore.acquire();
                    latch.countDown();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        boolean timedOut = !latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("Semaphore improperly acquired a permit!",timedOut);

        semaphore.release();
        boolean noTimeOut = latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("Semaphore did not release a permit for another party!",noTimeOut);
        errorFuture.get();
    }

    @Test(timeout = 1500l)
    public void testRunOutOfPermitsTwoClients() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final ZkSemaphore semaphore = new ZkSemaphore(1,basePath,zkSessionManager);
        semaphore.acquire();
        Future<Void> errorFuture = testService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ZooKeeper zk = newZooKeeper();
                try {
                    ZkSemaphore semaphore2 = new ZkSemaphore(1, basePath, new BaseZkSessionManager(zk));
                    semaphore2.acquire();
                    latch.countDown();
                } finally {
                    zk.close();
                }
                return null;
            }
        });

        boolean timedOut = !latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("Semaphore improperly acquired a permit!",timedOut);

        semaphore.release();
        boolean noTimeOut = latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("Semaphore did not release a permit for another party!",noTimeOut);

        errorFuture.get();
    }

    @Test(timeout = 1000l)
    public void testAcquireMultiplePermits() throws Exception{
        ZkSemaphore semaphore = new ZkSemaphore(2,basePath,zkSessionManager);

        semaphore.acquire(2);
        assertEquals("Incorrect number of permits reported!",0,semaphore.availablePermits());

        semaphore.release();
        assertEquals("Incorrect number of permits reported",1,semaphore.availablePermits());

        semaphore.release();
        assertEquals("Incorrect number of permits reported",2,semaphore.availablePermits());
    }

    @Test(timeout = 2000l)
    public void testAcquireMultiplePermitsBlocksUntilAvailableTwoThreads() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final ZkSemaphore semaphore = new ZkSemaphore(2,basePath,zkSessionManager);

        semaphore.acquire(2);
        assertEquals("Incorrect number of permits reported!",0,semaphore.availablePermits());
        Future<Void> errorFuture = testService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                semaphore.acquire(2);
                latch.countDown();
                return null;
            }
        });

        boolean timedOut1 = !latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("multiple permits were incorrectly acquired",timedOut1);
        semaphore.release();

        boolean timedOut2 = !latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("multiple permits were incorrectly acquired",timedOut2);

        semaphore.release();
        boolean timedOut3 = latch.await(500,TimeUnit.MILLISECONDS);
        assertTrue("multiple permits were incorrectly released",timedOut3);
        errorFuture.get();
    }

    @Test(timeout = 2000l)
    public void testAcquireMultiplePermitsBlocksUntilAvailableTwoClients() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final ZkSemaphore semaphore = new ZkSemaphore(2,basePath,zkSessionManager);

        semaphore.acquire();
        semaphore.acquire();
        assertEquals("Incorrect number of permits reported!",0,semaphore.availablePermits());
        Future<Void> errorFuture = testService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ZooKeeper zk = newZooKeeper();
                try {
                    ZkSemaphore semaphore2 = new ZkSemaphore(2, basePath, new BaseZkSessionManager(zk));
                    semaphore2.acquire(2);
                    latch.countDown();
                } finally {
                    zk.close();
                }
                return null;
            }
        });

        boolean timedOut1 = !latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("multiple permits were incorrectly acquired",timedOut1);

        semaphore.release();
        boolean timedOut2 = !latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("multiple permits were incorrectly acquired",timedOut2);

        semaphore.release();
        boolean timedOut3 = latch.await(500,TimeUnit.MILLISECONDS);
        assertTrue("multiple permits were incorrectly released",timedOut3);
        errorFuture.get();
    }

    @Test(timeout = 1000l)
    public void testReleaseMultiplePermits() throws Exception{
        ZkSemaphore semaphore = new ZkSemaphore(2,basePath,zkSessionManager);

        semaphore.acquire();
        semaphore.acquire();
        assertEquals("Incorrect number of permits reported!",0,semaphore.availablePermits());

        semaphore.release(2);
        assertEquals("Incorrect number of permits reported",2,semaphore.availablePermits());
    }

    @Test(timeout = 1000l)
    public void testTryAcquire() throws Exception{
        ZkSemaphore semaphore = new ZkSemaphore(2,basePath,zkSessionManager);
        assertTrue("Semaphore did not acquire!",semaphore.tryAcquire());
    }

    @Test(timeout = 1000l)
    public void testTryAcquireTimesOut() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final ZkSemaphore semaphore = new ZkSemaphore(1,basePath,zkSessionManager);
        assertTrue("Semaphore did not acquire!",semaphore.tryAcquire());

        Future<Void> errorFuture = testService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                semaphore.tryAcquire(500, TimeUnit.MILLISECONDS);
                latch.countDown();
                return null;
            }
        });

        boolean noTimeOut = latch.await(800, TimeUnit.MILLISECONDS);
        assertTrue("Semaphore did not appropriately time out",noTimeOut);
        errorFuture.get();
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
