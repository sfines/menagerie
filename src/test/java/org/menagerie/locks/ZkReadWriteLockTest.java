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

import org.apache.zookeeper.ZooDefs;
import org.junit.Test;
import org.menagerie.MenagerieTest;
import org.menagerie.util.TestingThreadFactory;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author Scott Fines
 * @version 1.0
 */
public class ZkReadWriteLockTest extends MenagerieTest {

    private static final ExecutorService testService = Executors.newFixedThreadPool(2, new TestingThreadFactory());


    @Override
    protected void prepare() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected void close() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Test(timeout = 1000l)
    public void testNoWritesWithASingleRead() throws Exception{
        ReadWriteLock lock = new ReentrantZkReadWriteLock(testPath, zkSessionManager);

        Lock readLock = lock.readLock();
        final Lock writeLock = lock.writeLock();
        readLock.lock();
        final CountDownLatch latch = new CountDownLatch(1);

        testService.submit(new Runnable() {
            @Override
            public void run() {
                writeLock.lock();
                try{
                    latch.countDown();
                }finally{
                    writeLock.unlock();
                }
            }
        });

        boolean acquired = latch.await(250, TimeUnit.MILLISECONDS);
        assertTrue("The Write lock was improperly acquired!",!acquired);

        //unlock the read lock, and make sure that the write lock is acquired
        readLock.unlock();
        acquired = latch.await(250, TimeUnit.MILLISECONDS);
        assertTrue("The Write lock was never acquired!",acquired);
    }

    @Test(timeout = 1000l)
    public void testNoReadsWithSingleWrite() throws Exception{
        ReadWriteLock lock = new ReentrantZkReadWriteLock(testPath, zkSessionManager);
        final CyclicBarrier waitBarrier = new CyclicBarrier(2);

        final Lock readLock = lock.readLock();
        final Lock writeLock = lock.writeLock();

        final CountDownLatch latch = new CountDownLatch(1);
        Future<Void> future = testService.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                waitBarrier.await();

                readLock.lock();
                try{
                    latch.countDown();
                }finally{
                    readLock.unlock();
                }
                return null;  //To change body of implemented methods use File | Settings | File Templates.
            }
        });

        writeLock.lock();
        waitBarrier.await();

        boolean acquired = latch.await(250, TimeUnit.MILLISECONDS);
        assertTrue("The Read lock was improperly acquired!",!acquired);

        //unlock the read lock, and make sure that the write lock is acquired

        writeLock.unlock();
        acquired = latch.await(300, TimeUnit.MILLISECONDS);
        assertTrue("The Read lock was never acquired!",acquired);

        //make sure no exceptions were thrown
        future.get();
    }

    @Test(timeout = 1000l)
    public void testMultipleReadsAllowed() throws Exception{
        ReadWriteLock firstLock = new ReentrantZkReadWriteLock(testPath, zkSessionManager);
        ReadWriteLock secondLock = new ReentrantZkReadWriteLock(testPath, zkSessionManager);

        final Lock firstReadLock = firstLock.readLock();
        final Lock secondReadLock = secondLock.readLock();
        final CountDownLatch correctnessLatch = new CountDownLatch(2);
        testService.submit(new Runnable() {
            @Override
            public void run() {
                firstReadLock.lock();
                try{
                    correctnessLatch.countDown();
                }finally{
                    firstReadLock.unlock();
                }
            }
        });

        testService.submit(new Runnable() {
            @Override
            public void run() {
                secondReadLock.lock();
                try{
                    correctnessLatch.countDown();
                }finally{
                    secondReadLock.unlock();
                }
            }
        });

        boolean doubleAcquired = correctnessLatch.await(2, TimeUnit.SECONDS);
        assertTrue("Two Read locks were not acquired concurrently!",doubleAcquired);
    }

    @Test(timeout = 1000l)
    public void testOneWriteAllowed() throws Exception{
        ReadWriteLock firstLock = new ReentrantZkReadWriteLock(testPath, zkSessionManager);
        ReadWriteLock secondLock = new ReentrantZkReadWriteLock(testPath, zkSessionManager);

        final Lock firstWriteLock = firstLock.writeLock();
        final Lock secondWriteLock = secondLock.writeLock();
        firstWriteLock.lock();
        final CountDownLatch correctnessLatch = new CountDownLatch(1);

        testService.submit(new Runnable() {
            @Override
            public void run() {
                secondWriteLock.lock();
                try{
                    correctnessLatch.countDown();
                }finally{
                    secondWriteLock.unlock();
                }
            }
        });

        boolean acquired = correctnessLatch.await(250, TimeUnit.MILLISECONDS);
        assertTrue("Two Write locks were acquired concurrently!",!acquired);

        //unlock and check
        firstWriteLock.unlock();

        acquired = correctnessLatch.await(250, TimeUnit.MILLISECONDS);
        assertTrue("The second write lock was never acquired!",acquired);
    }

    @Test(timeout=1000l)
    public void testUpgradingReadToWriteNotPossibleSingleThread() throws Exception{
        ReadWriteLock lock = new ReentrantZkReadWriteLock(testPath,zkSessionManager,ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Lock readLock = lock.readLock();
        readLock.lock();

        boolean acquired = lock.writeLock().tryLock(250l, TimeUnit.MILLISECONDS);
        assertTrue("The Write lock was acquired improperly!",!acquired);
    }

    @Test(timeout = 100000l)
    public void testDowngradingWriteLockToReadPossible() throws Exception{
        ReentrantZkReadWriteLock lock = new ReentrantZkReadWriteLock(testPath,zkSessionManager,ZooDefs.Ids.OPEN_ACL_UNSAFE);
        Lock writeLock = lock.writeLock();
        writeLock.lock();

        boolean acquired = lock.readLock().tryLock(250l, TimeUnit.MILLISECONDS);
        assertTrue("The Read lock was never acquired!",acquired);
        writeLock.unlock();

        //assert that the ReadLock still has it
        boolean hasRead = lock.readLock().hasLock();
        assertTrue("The Read lock does not have the lock anymore!",hasRead);
    }



}
