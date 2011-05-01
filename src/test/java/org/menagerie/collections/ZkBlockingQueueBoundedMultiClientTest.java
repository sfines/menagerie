/*
 * Copyright 2010 Scott Fines
 * <p>
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
package org.menagerie.collections;

import org.apache.zookeeper.ZooKeeper;
import org.junit.AfterClass;
import org.junit.Test;
import org.menagerie.BaseZkSessionManager;
import org.menagerie.JavaSerializer;
import org.menagerie.MenagerieTest;
import org.menagerie.Serializer;
import org.menagerie.util.TestingThreadFactory;

import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 29-Jan-2011
 *          Time: 16:25:05
 */
public class ZkBlockingQueueBoundedMultiClientTest extends MenagerieTest{
    ZkBlockingQueue<String> testQueue;

    Serializer<String> serializer = new JavaSerializer<String>();

    private static final ExecutorService service = Executors.newFixedThreadPool(2,new TestingThreadFactory());
    private static final int bound = 5;
    @AfterClass
    public static void shutdownAll(){
        service.shutdownNow();
    }

    @Override
    protected void prepare() {
        testQueue = new ZkBlockingQueue<String>(testPath,serializer,zkSessionManager,bound);
    }

    @Override
    protected void close() {
        testQueue = null;
    }

    @Test(timeout = 1000l)
    public void testTakeBlocksUntilElementBecomesAvailable() throws Exception{
        final CyclicBarrier startLatch = new CyclicBarrier(2);
        final String testInput="test";

        Future<String> takeFuture = service.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                ZooKeeper myZk = newZooKeeper();
                try{
                    ZkBlockingQueue<String> myQueue = new ZkBlockingQueue<String>(testPath, serializer, new BaseZkSessionManager(myZk), bound);
                    startLatch.await();

                    //this should block until the barrier is entered elsewhere
                    System.out.println(Thread.currentThread().getName()+": Taking an element off the queue...");
                    String queueEntry = myQueue.take();
                    System.out.println(Thread.currentThread().getName()+": Element "+ queueEntry+" taken from queue");
                    return queueEntry;
                }finally{
                    myZk.close();
                }
            }
        });

        //tell the other thread to begin the take operation
        startLatch.await();
        try{
            takeFuture.get(250, TimeUnit.MILLISECONDS);
            fail("take returned before timing out!");
        }catch(TimeoutException te){
            //this is supposed to happen, so just continue on
        }

        //add an entry to make the future proceed
        testQueue.put(testInput);
        String tookValue = takeFuture.get(500, TimeUnit.MILLISECONDS);
        assertEquals("took value is incorrect!",testInput,tookValue);
        assertEquals("Reported Queue Size is incorrect!",0,testQueue.size());
    }

    @Test(timeout = 1000l)
    public void testPollBlocksUntilElementBecomesAvailable() throws Exception{
         final CyclicBarrier startLatch = new CyclicBarrier(2);
        final String testInput="test";

        Future<String> takeFuture = service.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                ZooKeeper myZk = newZooKeeper();
                try{
                    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection"})
                    ZkBlockingQueue<String> myQueue = new ZkBlockingQueue<String>(testPath, serializer, new BaseZkSessionManager(myZk), bound);
                    startLatch.await();

                    //this should block until the barrier is entered elsewhere
                    System.out.println(Thread.currentThread().getName()+": Polling an element off the queue...");
                    String queueEntry = myQueue.poll(Long.MAX_VALUE,TimeUnit.DAYS);
                    System.out.println(Thread.currentThread().getName()+": Element "+ queueEntry+" taken from queue");
                    return queueEntry;
                }finally{
                    myZk.close();
                }
            }
        });

        //tell the other thread to begin the take operation
        startLatch.await();
        try{
            takeFuture.get(250, TimeUnit.MILLISECONDS);
            fail("take returned before timing out!");
        }catch(TimeoutException te){
            //this is supposed to happen, so just continue on
        }

        //add an entry to make the future proceed
        testQueue.put(testInput);
        String tookValue = takeFuture.get(500, TimeUnit.MILLISECONDS);
        assertEquals("took value is incorrect!",testInput,tookValue);
        assertEquals("Reported Queue Size is incorrect!",0,testQueue.size());
    }


    @Test(timeout = 1000l)
    public void testOfferBlocksIfQueueIsFull() throws Exception{
        fillQueue();

        Future<Boolean> offerFuture = service.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                ZooKeeper myZk = newZooKeeper();
                try{
                    ZkBlockingQueue<String> myQueue = new ZkBlockingQueue<String>(testPath, serializer, new BaseZkSessionManager(myZk), bound);
                    //this should block
                    return myQueue.offer("Test String",Long.MAX_VALUE,TimeUnit.DAYS);
                }finally{
                    myZk.close();
                }
            }
        });

        try{
            offerFuture.get(400,TimeUnit.MILLISECONDS);
            fail("Offer does not block!");
        }catch(TimeoutException te){
            //this is what we want to happen, so ignore
        }

        //remove an item from the queue to allow a thread to proceed
        testQueue.poll();

        Boolean shouldBeTrue = offerFuture.get(100, TimeUnit.MILLISECONDS);//should happen right away
        assertTrue("Offer did not succeed!",shouldBeTrue);
    }

    private void fillQueue() {
        //fill the queue
        for(int i=0;i<bound;i++){
            testQueue.offer(Integer.toString(i));
        }
        assertEquals("Queue did not fill correctly!",0,testQueue.remainingCapacity());
    }
}
