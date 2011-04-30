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

import org.junit.Test;
import org.menagerie.MenagerieTest;
import org.menagerie.Serializer;
import org.menagerie.util.TestingThreadFactory;

import java.util.Map;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 29-Jan-2011
 *          Time: 10:43:35
 */
public class ZkHashMapTestMultiThreaded extends MenagerieTest{
    private static ZkHashMap<String,String> testMap;
    private static Serializer<Map.Entry<String,String>> serializer;

    private static ExecutorService service = Executors.newFixedThreadPool(2,new TestingThreadFactory());

    @Override
    protected void prepare() {
        serializer = new JavaEntrySerializer<String, String>();
        testMap = new ZkHashMap<String, String>(testPath,zkSessionManager,serializer);
    }

    @Override
    protected void close() {
        //no-op
    }

    @Test(timeout = 1000l)
    public void testPutAndGetTwoThreads() throws Exception{
        final String testKey = "Test Key";
        final String testValue = "Test Value";
        final CyclicBarrier waiter = new CyclicBarrier(3);
        final CountDownLatch latch = new CountDownLatch(2);

        Future<String> getFuture = service.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                waiter.await(); //wait until all threads are ready to test

                String myTestValue=null;
                while(myTestValue==null){
                    System.out.println("Calling get!");
                    myTestValue = testMap.get(testKey);
                    System.out.println("Get returned correctly!");
                }
                return myTestValue;
            }
        });

        Future<Void> putFuture = service.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                waiter.await();
                System.out.println("Calling put!");
                //put an entry into the map
                testMap.put(testKey,testValue);
                System.out.println("put returned correctly");
                return null;
            }
        });

        waiter.await(); //now everyone is waiting

        //now check for exceptions, and that the correct value was returned from the getFuture
        putFuture.get(); //check for exceptions

        String testGetValue = getFuture.get();
        assertEquals("Test value returned is incorrect!",testValue,testGetValue);
    }

    @Test(timeout = 1000l)
    public void testPutIfAbsentContended() throws Exception{
       final String testKey = "Test Key";
        final String testValue1 = "Test Value 1";
        final String testValue2 = "Test Value 2";
        final CyclicBarrier waiter = new CyclicBarrier(3);

        assertEquals("There are elements in the map still!",0,testMap.size());
        Future<Boolean> thread1Future = service.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                waiter.await(); //wait until all threads are ready to test

                //call put-if-absent and see if I won!
                String myReturnedTestValue = testMap.putIfAbsent(testKey, testValue1);
                return myReturnedTestValue.equals(testValue1);
            }
        });

        Future<Boolean> thread2Future = service.submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                waiter.await(); //wait until all threads are ready to test

                //call put-if-absent and see if I won!
                String myReturnedTestValue = testMap.putIfAbsent(testKey, testValue2);
                return myReturnedTestValue.equals(testValue2);
            }
        });

        waiter.await(); //now everyone is waiting

        //now check for exceptions, and that the correct value was returned from the thread1Future
        boolean thread1Won = thread1Future.get();//check for exceptions
        boolean thread2Won = thread2Future.get();//check for exceptions

        if(thread1Won){
            //check to see that that is actually what is in the map
            getAndAssert(testKey, testValue1);
        }else if(thread2Won){
            getAndAssert(testKey,testValue2);
        }else{
            fail("Neither thread claims to have put a value!");
        }
    }

    private void getAndAssert(String key, String correctValue) {
        String actualValue = testMap.get(key);
        assertEquals("Incorrect actual value returned!",correctValue,actualValue);
    }
}
