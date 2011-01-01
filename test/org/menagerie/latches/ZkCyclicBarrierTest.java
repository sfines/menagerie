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
package org.menagerie.latches;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 09:34:14
 */
public class ZkCyclicBarrierTest {
    private static ZooKeeper zk;
    private static final String baseBarrierPath = "/test-barriers";
    private static final int timeout = 5000;
    private static final ExecutorService executor = Executors.newFixedThreadPool(3);

    private ZkCyclicBarrier cyclicBarrier;

    @Before
    public void setup() throws Exception {
        zk = newZooKeeper();

        //be sure that the lock-place is created
        try{
            zk.create(baseBarrierPath,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }catch(KeeperException ke){
            if(ke.code()!= KeeperException.Code.NODEEXISTS)
                throw ke;
        }
    }

    @After
    public void tearDown() throws Exception{
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

    @Test(timeout = 5000l)
    public void testBarrierWorks()throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(1, new BaseZkSessionManager(zk), baseBarrierPath);
        long position = cyclicBarrier.await();
        assertEquals("Position is incorrect!",0,position);
    }

    @Test(timeout = 5000l)
    public void testBarrierWorksWithTwoThreads() throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(2, new BaseZkSessionManager(zk), baseBarrierPath);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    cyclicBarrier.await();
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                } catch (BrokenBarrierException e) {
                    fail(e.getMessage());
                }
            }
        });

        cyclicBarrier.await();
    }

    @Test(timeout = 5000l)
    public void testBarrierWorksWithTwoClients() throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(2, new BaseZkSessionManager(zk), baseBarrierPath);

        ZooKeeper otherZooKeeper = newZooKeeper();
        ZkCyclicBarrier otherCyclicBarrier = new ZkCyclicBarrier(2, new BaseZkSessionManager(otherZooKeeper), baseBarrierPath);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    cyclicBarrier.await();
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                } catch (BrokenBarrierException e) {
                    fail(e.getMessage());
                }
            }
        });

        otherCyclicBarrier.await();
    }

    @Test(timeout = 5000l, expected = TimeoutException.class)
    public void testBarrierTimeOutThrowsTimeoutException() throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(2, new BaseZkSessionManager(zk), baseBarrierPath);
        assertEquals("Cyclic Barrier is not properly constructed!",1,zk.getChildren(baseBarrierPath,false).size());
        cyclicBarrier.await(500, TimeUnit.MILLISECONDS);
    }

    @Test(timeout = 1000l, expected = BrokenBarrierException.class)
    public void testBarrierTimeoutCausesBrokenBarrierOnOtherThread() throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(3, new BaseZkSessionManager(zk), baseBarrierPath);
        assertEquals("Cyclic Barrier is not properly constructed!",1,zk.getChildren(baseBarrierPath,false).size());

        executor.submit(new Runnable() {
            @Override
            public void run() {
                boolean thrown = false;
                try {
                    System.out.println(zk.getChildren(baseBarrierPath,false));
                    cyclicBarrier.await(500,TimeUnit.MILLISECONDS);
                    System.out.println(zk.getChildren(baseBarrierPath,false));
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                } catch (BrokenBarrierException e) {
                    fail(e.getMessage());
                } catch (TimeoutException e) {
                    thrown=true;
                } catch (KeeperException e) {
                    fail(e.getMessage());
                }
                assertTrue("TimeoutException was never thrown!",thrown);
            }
        });
        cyclicBarrier.await();
    }

    @Test(timeout = 1000l, expected = BrokenBarrierException.class)
    public void testBarrierTimeoutCausesBrokenBarrierOnOtherClients() throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(3, new BaseZkSessionManager(zk), baseBarrierPath);

        ZkCyclicBarrier otherCyclicBarrier = new ZkCyclicBarrier(3, new BaseZkSessionManager(newZooKeeper()), baseBarrierPath);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                boolean thrown = false;
                try {
                    cyclicBarrier.await(500,TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                } catch (BrokenBarrierException e) {
                    fail(e.getMessage());
                } catch (TimeoutException e) {
                    thrown=true;
                }
                assertTrue("TimeoutException was never thrown!",thrown);
            }
        });
        otherCyclicBarrier.await();
    }

    @Test(timeout = 1000l)
    public void testResetWorks() throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(1, new BaseZkSessionManager(zk), baseBarrierPath);
        cyclicBarrier.await();

        cyclicBarrier.reset();

    }

    @Test(timeout = 1000l, expected = BrokenBarrierException.class)
    public void testResetOnOtherThreadBreaksBarrier() throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(2, new BaseZkSessionManager(zk), baseBarrierPath);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                cyclicBarrier.reset();
            }
        });

        cyclicBarrier.await();
    }

    @Test(timeout = 1000l, expected = BrokenBarrierException.class)
    public void testResetOnOtherClientBreaksBarrier() throws Exception{
        cyclicBarrier = new ZkCyclicBarrier(3, new BaseZkSessionManager(zk), baseBarrierPath);

        ZooKeeper otherZooKeeper = newZooKeeper();
        ZkCyclicBarrier otherCyclicBarrier = new ZkCyclicBarrier(3, new BaseZkSessionManager(otherZooKeeper), baseBarrierPath);

        executor.submit(new Runnable() {
            @Override
            public void run() {
                cyclicBarrier.reset();
            }
        });

        otherCyclicBarrier.await();
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
