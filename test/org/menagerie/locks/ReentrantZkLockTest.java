/*
 * Copyright 2010 Scott Fines
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.menagerie.locks;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static junit.framework.Assert.assertTrue;

/**
 * Unit tests for ReentrantZkLock
 * <p>
 * Note: These methods will not run without first having a ZooKeeper server started on {@code hostString}.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 21-Nov-2010
 *          Time: 13:49:20
 */
public class ReentrantZkLockTest {

    private static final String hostString = "localhost:2181";
    private static final String baseLockPath = "/test-locks";
    private static final int timeout = 2000;
    private static final ExecutorService testService = Executors.newFixedThreadPool(2);

    private static ZooKeeper zk;
    private static ZkSessionManager zkSessionManager;

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

    @Test(timeout = 1500l)
    public void testConditionWaitsForSignalOtherThread() throws Exception{
        final Lock firstLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
        final Condition firstCondition = firstLock.newCondition();

        firstLock.lock();
        //fire off a thread that will signal the main process thread
        testService.submit(new Runnable() {
            @Override
            public void run() {
                firstLock.lock();
                System.out.println("Lock acquired on second thread");
                try{
                    firstCondition.signal();
                    System.out.println("Lock signalled on second thread");
                }finally{
                    System.out.println("Lock released on second thread");
                    firstLock.unlock();
                }
            }
        });

        //wait for signal notification
        System.out.println("First thread waiting for notification");
        firstCondition.await();
        System.out.println("First thread has been notified");
    }

    @Test(timeout = 1500l)
    public void testConditionWaitsForSignalOtherClient() throws Exception{
        final Lock firstLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
        final Condition firstCondition = firstLock.newCondition();

        firstLock.lock();
        //fire off a thread that will signal the main process thread
        testService.submit(new Runnable() {
            @Override
            public void run() {
                final Lock otherClientLock;
                try {
                    otherClientLock = ZkLocks.newReentrantLock(new BaseZkSessionManager(newZooKeeper()),baseLockPath);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                final Condition otherClientCondition = otherClientLock.newCondition();
                otherClientLock.lock();
                System.out.println("Lock acquired on second thread");
                try{
                    otherClientCondition.signal();
                    System.out.println("Lock signalled on second thread");
                }finally{
                    System.out.println("Lock released on second thread");
                    otherClientLock.unlock();
                }
            }
        });

        //wait for signal notification
        System.out.println("First thread waiting for notification");
        firstCondition.await();
        System.out.println("First thread has been notified");
        firstLock.unlock();
    }

    @Test(timeout = 1000l)
    public void testConditionTimesOut() throws Exception{
        Lock firstLock = ZkLocks.newReentrantLock(zkSessionManager,baseLockPath);
        Condition firstCondition = firstLock.newCondition();

        firstLock.lock();
        boolean timedOut = firstCondition.await(250l, TimeUnit.MILLISECONDS);
        assertTrue("Condition did not time out!",!timedOut);
        firstLock.unlock();
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
