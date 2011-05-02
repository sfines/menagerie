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
import org.junit.Ignore;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.util.TestingThreadFactory;
import org.omg.PortableServer.THREAD_POLICY_ID;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
public class ReentrantZkLock2Test {

    private static final String hostString = "localhost:2181";
    private static final String baseLockPath = "/test-locks";
    private static final int timeout = 2000;
    private static final ExecutorService testService = Executors.newFixedThreadPool(2, new TestingThreadFactory());

    private static ZooKeeper zk;
    private static ZkSessionManager zkSessionManager;

    @Before
    public void setup() throws Exception {
        zk = newZooKeeper();

        //be sure that the lock-place is created
        ZkUtils.recursiveSafeDelete(zk,baseLockPath,-1);
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
        Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
        firstLock.lock();
        try{
            testService.submit(new Runnable() {
                @Override
                public void run() {
                    Lock secondLock = new ReentrantZkLock(baseLockPath, zkSessionManager);
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
        Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
        firstLock.lock();
        try{
            testService.submit(new Runnable() {
                @Override
                public void run() {
                    Lock secondLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
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
        final Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
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
        final Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);


        firstLock.lock();
        Future<Void> errorFuture = testService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                ZooKeeper newZk = newZooKeeper();
                try {
                    final Lock sameLock = new ReentrantZkLock2(baseLockPath, new BaseZkSessionManager(newZk));
                    sameLock.lock();
                    try {
                        latch.countDown();
                    } finally {
                        sameLock.unlock();
                    }
                } finally {
                    newZk.close();
                }
                return null;
            }
        });

        boolean acquired = latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("The Lock was acquired twice by two separate threads!",!acquired);

        firstLock.unlock();

        acquired = latch.await(500, TimeUnit.MILLISECONDS);
        assertTrue("The Lock was never acquired by another thread!",acquired);
        errorFuture.get(); //check for errors
    }


    @Test(timeout = 1000l)
    public void testLockTimesOutWaiting() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
        firstLock.lock();
        try{
            testService.submit(new Runnable() {
                @Override
                public void run() {
                    Lock secondLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
                    try {
                        boolean acquired = secondLock.tryLock(300, TimeUnit.MILLISECONDS);
                        if(acquired){
                            try{
                                latch.countDown();
                            }finally{
                                secondLock.unlock();
                            }
                        }else{
                            System.out.println("Lock was not acquired in 300 milliseconds");
                        }
                    } catch (InterruptedException e) {
                        fail(e.getMessage());
                    }
                }
            });

            boolean acquired =latch.await(500, TimeUnit.MILLISECONDS);
            assertTrue("Lock was acquired too early!",!acquired);
        }finally{
            firstLock.unlock();
        }
    }

    @Test(timeout = 1000l)
    public void testLockInterruptiblyWorksDifferentInstances() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
        firstLock.lockInterruptibly();
        Future<Void>errorFuture;
        try{
            errorFuture = testService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    Lock secondLock = new ReentrantZkLock(baseLockPath, zkSessionManager);
                    secondLock.lockInterruptibly();
                    try {
                        latch.countDown();
                    } finally {
                        secondLock.unlock();
                    }
                    return null;
                }
            });

            boolean nowAcquired = latch.await(500, TimeUnit.MILLISECONDS);
            assertTrue("The Second lock was acquired before the first lock was released!",!nowAcquired);

        }finally{
            firstLock.unlock();
        }
        //make sure that no errors happen
        errorFuture.get();
    }

    @Test(timeout = 1000l)
    public void testLockInterruptiblyWorksSameInstance() throws Exception{
        final CountDownLatch latch = new CountDownLatch(1);
        final Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
        firstLock.lockInterruptibly();
        Future<Void>errorFuture;
        try{
            errorFuture = testService.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
//                    Lock secondLock = new ReentrantZkLock(baseLockPath, zkSessionManager);
                    firstLock.lockInterruptibly();
                    try {
                        latch.countDown();
                    } finally {
                        firstLock.unlock();
                    }
                    return null;
                }
            });

            boolean nowAcquired = latch.await(500, TimeUnit.MILLISECONDS);
            assertTrue("The Second lock was acquired before the first lock was released!",!nowAcquired);

        }finally{
            firstLock.unlock();
        }
        //make sure that no errors happen
        errorFuture.get();
    }

    @Test(timeout = 2000l)
    public void testLockInterruptiblyIsInterruptibleDifferentLockInstances() throws Exception{
        /*
        The idea here is to test to ensure that we can interrupt the
        acquisition of a lockInterruptibly call from another thread, using a different
        instance of the same distributed lock
        */
        Lock mainLock = new ReentrantZkLock2(baseLockPath,zkSessionManager);
        mainLock.lock();
        final CyclicBarrier latch = new CyclicBarrier(2);
        try{
            final AtomicBoolean bool = new AtomicBoolean();
            Thread t = new Thread() {
                @Override
                public void run() {
                    Lock secondaryLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
                    boolean thrown = false;
                    try {
                        System.out.printf("%s: Attempting to acquire the lock interruptibly%n",Thread.currentThread().getName());
                        latch.await();
                        secondaryLock.lockInterruptibly();
                        fail("secondary Lock was acquired improperly!");
                    } catch (InterruptedException ie) {
                        System.out.printf("%s: Secondary lock threw an InterruptedException$n",Thread.currentThread().getName());
                        thrown = true;
                    } catch (BrokenBarrierException e) {
                        fail(e.getMessage());
                    }
                    bool.set(thrown);
                }
            };
            t.setName("secondary-thread");
            t.start();
            //make sure that the thread is set
            latch.await();
            Thread.sleep(300);
            //now interrupt the thread
            System.out.printf("%s: Interrupting secondary thread: %s%n",Thread.currentThread().getName(),t.getName());
            t.interrupt();
            //now make sure that we get back true
            t.join();
            assertTrue("Thread was not interrupted successfully!",bool.get());
        }finally{
            mainLock.unlock();
        }
    }

    @Test(timeout = 2000l)
    public void testLockInterruptiblyIsInterruptibleSameLockInstance() throws Exception{
        /*
        The idea here is to test to ensure that we can interrupt the
        acquisition of a lockInterruptibly call from another thread, using a different
        instance of the same distributed lock
        */
        Lock mainLock = new ReentrantZkLock2(baseLockPath,zkSessionManager);
        mainLock.lock();
        final CyclicBarrier latch = new CyclicBarrier(2);
        try{
            final AtomicBoolean bool = new AtomicBoolean();
            Thread t = new Thread() {
                @Override
                public void run() {
                    Lock secondaryLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
                    boolean thrown = false;
                    try {
                        System.out.printf("%s: Attempting to acquire the lock interruptibly%n",Thread.currentThread().getName());
                        latch.await();
                        secondaryLock.lockInterruptibly();
                        fail("secondary Lock was acquired improperly!");
                    } catch (InterruptedException ie) {
                        System.out.printf("%s: Secondary lock threw an InterruptedException$n",Thread.currentThread().getName());
                        thrown = true;
                    } catch (BrokenBarrierException e) {
                        fail(e.getMessage());
                    }
                    bool.set(thrown);
                }
            };
            t.setName("secondary-thread");
            t.start();
            //make sure that the thread is set
            latch.await();
            Thread.sleep(300);
            //now interrupt the thread
            System.out.printf("%s: Interrupting secondary thread: %s%n",Thread.currentThread().getName(),t.getName());
            t.interrupt();
            //now make sure that we get back true
            t.join();
            assertTrue("Thread was not interrupted successfully!",bool.get());
        }finally{
            mainLock.unlock();
        }
    }

    @Test(timeout = 1500l)
    @Ignore("Ignored until Conditions are fully implemented in ReentrantZkLock2")
    public void testConditionWaitsForSignalOtherThread() throws Exception{
        final Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
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
    @Ignore("Ignored until Conditions are fully implemented in ReentrantZkLock2")
    public void testConditionWaitsForSignalOtherClient() throws Exception{
        final Lock firstLock = new ReentrantZkLock2(baseLockPath, zkSessionManager);
        final Condition firstCondition = firstLock.newCondition();

        firstLock.lock();
        //fire off a thread that will signal the main process thread
        testService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception{
                final Lock otherClientLock;
                ZooKeeper newZk = newZooKeeper();
                try {
                    otherClientLock = new ReentrantZkLock2(baseLockPath, new BaseZkSessionManager(newZk));
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
                }finally{
                    newZk.close();
                }
                return null;
            }
        });

        //wait for signal notification
        System.out.println("First thread waiting for notification");
        firstCondition.await();
        System.out.println("First thread has been notified");
        firstLock.unlock();
    }

    @Test(timeout = 1000l)
    @Ignore("Ignored until Conditions are fully implemented in ReentrantZkLock2")
    public void testConditionTimesOut() throws Exception{
        Lock firstLock = new ReentrantZkLock2(baseLockPath,zkSessionManager);
        Condition firstCondition = firstLock.newCondition();

        firstLock.lock();
        boolean timedOut = firstCondition.await(250l, TimeUnit.MILLISECONDS);
        assertTrue("Condition did not time out!",!timedOut);
        firstLock.unlock();
    }


    @Test(timeout = 10000l)
    public void testLockWorksUnderContentionDifferentClients() throws Exception{
        int numThreads = 5;
        final int numIterations = 100;
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        final CyclicBarrier startBarrier = new CyclicBarrier(numThreads+1);
        final CyclicBarrier endBarrier = new CyclicBarrier(numThreads+1);

        final UnsafeOperator operator = new UnsafeOperator();
        for(int i=0;i<numThreads;i++){
            service.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    startBarrier.await(); //make sure all threads are in the same place before starting

                    //create the lock that we're going to use
                    ZooKeeper zk = newZooKeeper();
                    try{
                        Lock testLock = new ReentrantZkLock2(baseLockPath,new BaseZkSessionManager(zk));
                        for(int j=0;j<numIterations;j++){
                            testLock.lock();
                            try{
                                operator.increment();
                            }finally{
                                testLock.unlock();
                            }
                        }
                    }finally{
                        zk.close();
                    }

                    //enter the end barrier to ensure that things are finished
                    endBarrier.await();
                    return null;
                }
            });
        }

        //start the test
        startBarrier.await();

        //wait for the end of the test
        endBarrier.await();

        //check that the number of operations that actually were recorded are correct
        int correctOps = numIterations*numThreads;
        assertEquals("Number of Operations recorded was incorrect!",correctOps,operator.getValue());
    }

    @Test(timeout = 10000l)
    public void testLockWorksUnderContentionSameClient() throws Exception{
        int numThreads = 5;
        final int numIterations = 100;
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
        final CyclicBarrier startBarrier = new CyclicBarrier(numThreads+1);
        final CyclicBarrier endBarrier = new CyclicBarrier(numThreads+1);

        final UnsafeOperator operator = new UnsafeOperator();
        final Lock testLock = new ReentrantZkLock2(baseLockPath,zkSessionManager);
        for(int i=0;i<numThreads;i++){
            service.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    startBarrier.await(); //make sure all threads are in the same place before starting

                    for(int j=0;j<numIterations;j++){
                        testLock.lock();
                        try{
                            operator.increment();
                        }finally{
                            testLock.unlock();
                        }
                    }

                    //enter the end barrier to ensure that things are finished
                    endBarrier.await();
                    return null;
                }
            });
        }

        //start the test
        startBarrier.await();

        //wait for the end of the test
        endBarrier.await();

        //check that the number of operations that actually were recorded are correct
        int correctOps = numIterations*numThreads;
        assertEquals("Number of Operations recorded was incorrect!",correctOps,operator.getValue());
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
