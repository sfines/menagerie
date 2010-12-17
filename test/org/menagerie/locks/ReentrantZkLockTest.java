package org.menagerie.locks;

import org.apache.zookeeper.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static junit.framework.Assert.assertTrue;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 21-Nov-2010
 *          Time: 13:49:20
 */
public class ReentrantZkLockTest {

    private static ZooKeeper zk;
    private static final String baseLockPath = "/test-locks";
    private static final int timeout = 200000;
    private static final ExecutorService testService = Executors.newFixedThreadPool(2);

    private static ZkSessionManager zkSessionManager;

    @BeforeClass
    public static void setupBeforeClass() throws Exception {

        zk = new ZooKeeper("localhost:2181", timeout,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });

        //be sure that the lock-place is created

        zk.create(baseLockPath,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zkSessionManager = new BaseZkSessionManager(zk);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception{
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

    @Test(timeout = 1500l)
    public void testOnlyOneLockAllowedTwoThreads()throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        Lock firstLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
        firstLock.lock();
        try{
            testService.submit(new Runnable() {
                @Override
                public void run() {
                    Lock secondLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
                    secondLock.lock();
                    try{
                        latch.countDown();
                    }finally{
                        secondLock.unlock();
                    }
                }
            });

            boolean nowAcquired = latch.await(500, TimeUnit.MILLISECONDS);
            assertTrue("The Second lock was acquired before the first lock was released!",!nowAcquired);
        }finally{
            firstLock.unlock();
        }
    }


    @Test(timeout = 1500l)
    public void testReentrancy()throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        Lock firstLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
        firstLock.lock();
        try{
            testService.submit(new Runnable() {
                @Override
                public void run() {
                    Lock secondLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
                    secondLock.lock();
                    try{
                        latch.countDown();
                    }finally{
                        secondLock.unlock();
                    }
                }
            });

            boolean nowAcquired = latch.await(500, TimeUnit.MILLISECONDS);
            assertTrue("The Second lock was acquired before the first lock was released!",!nowAcquired);

            //this should be fine
            firstLock.lock();
            System.out.println("Lock acquired twice!");
            firstLock.unlock();
            //should still be locked
            nowAcquired = latch.await(500, TimeUnit.MILLISECONDS);
            assertTrue("The Second lock was acquired before the first lock was released twice!",!nowAcquired);
        }finally{
            firstLock.unlock();
        }
    }

    @Test(timeout = 1500l)
    public void testMultipleThreadsCannotAccessSameLock()throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final Lock firstLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
        firstLock.lock();
        testService.submit(new Runnable() {
            @Override
            public void run() {
                firstLock.lock();
                try{
                    latch.countDown();
                }finally{
                    firstLock.unlock();
                }
            }
        });

        boolean acquired = latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("The Lock was acquired twice by two separate threads!",!acquired);

        firstLock.unlock();

        acquired = latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("The Lock was never acquired by another thread!",acquired);

    }

    @Test(timeout = 1000l)
    public void testMultipleClientsCannotAccessSameLock()throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final Lock firstLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
        final Lock sameLock = ZkLocks.newReentrantLock(new BaseZkSessionManager(newZooKeeper()),baseLockPath);

        firstLock.lock();
        testService.submit(new Runnable() {
            @Override
            public void run() {
                sameLock.lock();
                try{
                    latch.countDown();
                }finally{
                    sameLock.unlock();
                }
            }
        });

        boolean acquired = latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("The Lock was acquired twice by two separate threads!",!acquired);

        firstLock.unlock();

        acquired = latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("The Lock was never acquired by another thread!",acquired);
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
