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

import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Test;
import org.menagerie.JavaSerializer;
import org.menagerie.MenagerieTest;
import org.menagerie.ZkUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 20-Jan-2011
 *          Time: 08:39:05
 */
public class ZkBlockingQueueUnboundedSingleThread extends MenagerieTest{

    private BlockingQueue<String> testQueue;
    private static final String testElement = "Test element";

    @Override
    protected void prepare() {
        testQueue = new ZkBlockingQueue<String>(testPath, new JavaSerializer<String>(), zkSessionManager);
    }

    @Override
    protected void close() {
        //no-op, we're good
    }

    @After
    public void tearDown() throws Exception{
        try{
           ZkUtils.recursiveSafeDelete(zk, testPath,-1);

        }catch(KeeperException ke){
            //suppress because who cares what went wrong after our tests did their thing?
        }finally{
            zk.close();
        }
    }

    @Test(timeout = 1000l)
    public void testPutAndPeekAndPoll()throws Exception{
        testQueue.put(testElement);
        int reportedSize = testQueue.size();
        assertEquals("The Reported size is incorrect!",1,reportedSize);

        String peekedElement = testQueue.peek();
        assertEquals("The peeked object does not match the put object!",testElement,peekedElement);

        assertEquals("Peeking caused the reported size to be incorrect!",1,reportedSize);

        String polledElement = testQueue.poll();
        assertEquals("The polled object does not match the put object!",testElement,polledElement);
        assertEquals("Polling caused the reported size to be incorrect!",0,testQueue.size());
    }

    @Test(timeout = 1000l)
    public void testPollGetsEntriesInCorrectOrder() throws Exception{
        testQueue.put(testElement+"0");
        testQueue.put(testElement+"1");
        assertEquals("The Reported size is incorrect!",2,testQueue.size());

        String firstEntry = testQueue.poll();
        assertEquals("The first returned entry is not correct!",testElement+"0",firstEntry);

        String secondEntry = testQueue.poll();
        assertEquals("The first returned entry is not correct!",testElement+"1",secondEntry);
    }

    @Test(timeout = 1000l)
    public void testOfferAndPeekAndPoll() throws Exception{
        testQueue.offer(testElement);
        int reportedSize = testQueue.size();
        assertEquals("The Reported size is incorrect!",1,reportedSize);

        String peekedElement = testQueue.peek();
        assertEquals("The peeked object does not match the put object!",testElement,peekedElement);

        assertEquals("Peeking caused the reported size to be incorrect!",1,reportedSize);

        String polledElement = testQueue.poll();
        assertEquals("The polled object does not match the put object!",testElement,polledElement);
        assertEquals("Polling caused the reported size to be incorrect!",0,testQueue.size());
    }

    @Test(timeout = 1000l)
    public void testAddAndPeekAndPoll() throws Exception{
        testQueue.add(testElement);
        int reportedSize = testQueue.size();
        assertEquals("The Reported size is incorrect!",1,reportedSize);

        String peekedElement = testQueue.peek();
        assertEquals("The peeked object does not match the put object!",testElement,peekedElement);

        assertEquals("Peeking caused the reported size to be incorrect!",1,reportedSize);

        String polledElement = testQueue.poll();
        assertEquals("The polled object does not match the put object!",testElement,polledElement);
        assertEquals("Polling caused the reported size to be incorrect!",0,testQueue.size());
    }

    @Test(timeout = 1000l)
    public void testIteration() throws Exception{
        //put a couple of entries on the queue
        int size = 5;
        List<String> correctElements = new ArrayList<String>(size);
        for(int i=0;i<size;i++){
            testQueue.put(testElement+i);
            correctElements.add(testElement+i);
        }

        //get the testQueue's iterator and iterate over it, making sure that everything gets returned
        //and that the iterator doesn't madly fail
        Iterator<String> iterator = testQueue.iterator();
        int pos=0;
        while(iterator.hasNext()){
            assertEquals("Iterator returned elements in the incorrect order, or did not return correct elements",correctElements.get(pos),iterator.next());
            pos++;
        }
    }

    @Test(timeout = 1000l)
    public void testIteratorRemoval() throws Exception{
        //put a couple of entries on the queue
        int size = 5;
        List<String> correctElements = new ArrayList<String>(size);
        for(int i=0;i<size;i++){
            testQueue.put(testElement+i);
            correctElements.add(testElement+i);
        }

        //get the testQueue's iterator and iterate over it, making sure that everything gets returned
        //and that the iterator doesn't madly fail
        Iterator<String> iterator = testQueue.iterator();
        int pos=0;
        while(iterator.hasNext()){
            assertEquals("Iterator returned elements in the incorrect order, or did not return correct elements",correctElements.get(pos),iterator.next());
            if(pos%2==0){
                iterator.remove();
                correctElements.remove(pos);
            }else
                pos++;

        }
    }

    @Test(timeout = 1000l)
    public void testPollTimesOut() throws Exception{
        //test that poll times out if the queue is empty
        assertEquals("Queue is not reporting empty!",0,testQueue.size());

        String nullElement = testQueue.poll(500l, TimeUnit.MILLISECONDS);
        assertTrue("poll returned an element!",nullElement==null);
    }

    @Test(timeout = 1000l)
    public void testTimedPollSuccessfullyReturns() throws Exception{
        assertEquals("Queue is not reporting empty!",0,testQueue.size());

        testQueue.put("Test Element");

        //do a timed take, and make sure it is correct
        String retString = testQueue.poll(5, TimeUnit.SECONDS);
        assertEquals("Test Element",retString);
    }

    @Test(timeout = 1000l)
    public void testTimedPollSuccessfullyReturnsAfterPeek() throws Exception{
        assertEquals("Queue is not reporting empty!",0,testQueue.size());

        testQueue.put("Test Element");

        //first do a peek, and see what happens
        String retString = testQueue.peek();
        assertEquals("Test Element",retString);
        //do a timed take, and make sure it is correct
        retString = testQueue.poll(5, TimeUnit.SECONDS);
        assertEquals("Test Element",retString);
    }


}
