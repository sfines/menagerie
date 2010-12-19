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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import static org.junit.Assert.assertTrue;

/**
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 21-Nov-2010
 *          Time: 17:46:36
 */
public class ZkReadWriteLockTest {

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

    @Test(timeout = 1000l)
    public void testNoWritesWithASingleRead() throws Exception{
        ReadWriteLock lock = ZkLocks.newReadWriteLock(zkSessionManager,baseLockPath);

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
        ReadWriteLock lock = ZkLocks.newReadWriteLock(zkSessionManager,baseLockPath);

        final Lock readLock = lock.readLock();
        final Lock writeLock = lock.writeLock();
        writeLock.lock();
        final CountDownLatch latch = new CountDownLatch(1);
        testService.submit(new Runnable() {
            @Override
            public void run() {
                readLock.lock();
                latch.countDown();
                readLock.unlock();
            }
        });

        boolean acquired = latch.await(250, TimeUnit.MILLISECONDS);
        assertTrue("The Read lock was improperly acquired!",!acquired);

        //unlock the read lock, and make sure that the write lock is acquired

        writeLock.unlock();
        acquired = latch.await(250, TimeUnit.MILLISECONDS);
        assertTrue("The Read lock was never acquired!",acquired);


    }

    @Test(timeout = 1000l)
    public void testMultipleReadsAllowed() throws Exception{
        ReadWriteLock firstLock = ZkLocks.newReadWriteLock(zkSessionManager, baseLockPath);
        ReadWriteLock secondLock = ZkLocks.newReadWriteLock(zkSessionManager,baseLockPath);

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
        ReadWriteLock firstLock = ZkLocks.newReadWriteLock(zkSessionManager, baseLockPath);
        ReadWriteLock secondLock = ZkLocks.newReadWriteLock(zkSessionManager,baseLockPath);

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

}
