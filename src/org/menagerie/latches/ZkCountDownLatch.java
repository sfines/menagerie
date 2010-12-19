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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.locks.ReentrantZkLock;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

/**
 * ZooKeeper-based implementation of a CountDownLatch.
 * <p>
 * Wherever possible, this implementation is semantically equivalent to {@link java.util.concurrent.CountDownLatch},
 * and adheres to as many of the same constraints as are reasonable to expect. Where this implementation differs,
 * a notation will be made in the javadoc for that method.
 * <p>
 * Note that, unlike the concurrent version of this class, multiple uses of this class is allowed, so long as
 * {@link #closeLatch()} is called between uses.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Dec-2010
 *          Time: 09:42:27
 */
public class ZkCountDownLatch extends AbstractZkBarrier {
    private static final String latchPrefix = "countDownLatch";

    /**
     * Creates a new CountDownLatch on the specified latchNode, or joins a CountDownLatch which has been
     * previously created by another node/thread on the same latchNode.
     * <p>
     * When this constructor returns, the Latch is guaranteed to be in a cluster- and thread-safe state which
     * is ready to be used.
     *
     * @param total the number of elements which must countDown before threads may proceed.
     * @param latchNode the node to execute the latch under
     * @param zkSessionManager the ZkSessionManager to use
     * @param privileges the privileges for this latch
     * @throws RuntimeException wrapping:
     * <ul>
     *  <li> {@link org.apache.zookeeper.KeeperException} if the ZooKeeper Server has trouble with the requests
     *  <li> {@link InterruptedException} if the ZooKeeper client has trouble communicating with the ZooKeeper service
     * </ul>
     */
     ZkCountDownLatch(long total,String latchNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        super(total, latchNode, zkSessionManager, privileges);

        ensureState();
    }

    /**
     * Creates a new CountDownLatch on the specified latchNode, or joins a CountDownLatch which has been
     * previously created by another node/thread on the same latchNode, using Open, unsafe ACL privileges.
     * <p>
     * When this constructor returns, the Latch is guaranteed to be in a cluster- and thread-safe state which
     * is ready to be used.
     *
     * @param total the number of elements which must countDown before threads may proceed.
     * @param latchNode the node to execute the latch under
     * @param zkSessionManager the ZkSessionManager to use
     * @throws RuntimeException wrapping:
     * <ul>
     *  <li> {@link org.apache.zookeeper.KeeperException} if the ZooKeeper Server has trouble with the requests
     *  <li> {@link InterruptedException} if the ZooKeeper client has trouble communicating with the ZooKeeper service
     * </ul>
     */
    public ZkCountDownLatch(long total, String latchNode, ZkSessionManager zkSessionManager) {
        this(total,latchNode,zkSessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }


    /**
     *  Decrements the count of the latch, releasing all waiting parties when the count reaches zero.
     * <p>
     * If the current count of the latch is zero, this has no effect.
     * @throws RuntimeException wrapping:
     * <ul>
     *  <li> {@link org.apache.zookeeper.KeeperException} if the ZooKeeper Server has trouble with the requests
     *  <li> {@link InterruptedException} if the ZooKeeper client has trouble communicating with the ZooKeeper service
     * </ul>
     */
    public void countDown(){
        try{
            /*
            Must be Persistent_Sequential here instead of Ephemeral Sequential, because if a party counts down, then
            the zookeeper client loses its connection, the count down needs to not be lost
             */
           zkSessionManager.getZooKeeper().create(baseNode+"/"+latchPrefix+'-',emptyNode,privileges,CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the current count of the latch.
     * <p>
     * Note that the returned value constitutes a snapshot of the state of the Latch at the time of invocation--It is
     * possible that the current count of the latch has changed during the execution of this method.
     * 
     * @return the current count of the latch.
     * @throws RuntimeException wrapping:
     * <ul>
     *  <li> {@link org.apache.zookeeper.KeeperException} if the ZooKeeper Server has trouble with the requests
     *  <li> {@link InterruptedException} if the ZooKeeper client has trouble communicating with the ZooKeeper service
     * </ul>
     */
    public long getCount(){
        try {
            List<String> children = ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,false),latchPrefix);
            return children.size();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }



    /**
     * Causes the current thread to wait until the latch has counted down to zero, or until the current thread is
     * interrupted.
     * <p>
     * If the current count of the latch is zero, this method returns immediately.
     * <p>
     * If the current count of the latch is non-zero, the thread is disabled and lies dormant until one of two
     * things happen:
     * <ol>
     *  <li>the count reaches zero
     *  <li>The current thread gets interrupted
     * </ol>
     * <p>
     * If the current thread:
     * <ul>
     *  <li> has its interrupted status set on entry to this method; or
     *  <li> is interrupted while waiting
     * </ul>
     * Then an {@link InterruptedException} is thrown and the thread's current Interrupted status is cleared.
     *
     * @throws InterruptedException if the current thread is interrupted, or if communication between the ZooKeeper
     * client and server fails in some way
     */
    public void await() throws InterruptedException{
        await(Long.MAX_VALUE, TimeUnit.MINUTES);
    }

    /**
     * Causes the current thread to wait until the latch has counted down to zero, the current thread is
     * interrupted, or the specified timeout occurs.
     *
     * <p>
     * If the current count of the latch is zero, this method returns immediately
     * <p>
     * If the current count of the latch is non-zero, the thread is disabled and lies dormant until one of three
     * things happen:
     * <p>
     *  1. The count reaches zero<br/>
     *  2. The current thread gets interrupted<br/>
     *  3. The specified timeout is reached without the latch counting down fully.
     * <p>
     * If the current thread:
     * <ul>
     *  <li> has its interrupted status set on entry to this method; or
     *  <li> is interrupted while waiting
     * </ul>
     * Then an {@link InterruptedException} is thrown and the thread's current Interrupted status is cleared.
     * <p>
     * If the specified waiting time elapses then the value {@code false} is returned. If the timne is less than
     * or equals to zero, the method will not wait at all.
     *
     * @param timeout the maximum time to wait, in {@code unit} units.
     * @param unit the TimeUnit to use
     * @return true if the latch counted down fully before the timeout was reached, false otherwise. 
     * @throws InterruptedException if the current thread is interrupted, or if communication between the ZooKeeper
     * client and server fails in some way
     * @throws RuntimeException wrapping a {@link org.apache.zookeeper.KeeperException} if the ZooKeeper server goes
     *          wrong.
     */
    public boolean await(long timeout, TimeUnit unit)throws InterruptedException{
        return doWait(timeout,unit,latchPrefix);
    }

    /**
     * Closes the latch, signalling that this Latch has been expended.
     * <p>
     * When this method is finished, all latch nodes relating to this latch will be removed, and the
     * state of the latchNode will be ready to accept new latches.
     * <p>
     * This method is here to allow callers to ensure the clean destruction of latches in the case where a latch
     * uses permanent nodes.
     *
     *@throws RuntimeException wrapping:
     * <ul>
     *  <li> {@link org.apache.zookeeper.KeeperException} if the ZooKeeper Server has trouble with the requests
     *  <li> {@link InterruptedException} if the ZooKeeper client has trouble communicating with the ZooKeeper service
     * </ul>
     *
     */
    public void closeLatch() {
        ensureNodeExists();
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        //TODO -sf- is there a non-blocking way to do this?
        Lock lock = new ReentrantZkLock(baseNode, zkSessionManager, privileges);
        try {
            lock.lock();
            clearState(zooKeeper,latchPrefix);
            String readyNode = baseNode + "/countDown-latchReady";
            if(zooKeeper.exists(readyNode,false)!=null){
                ZkUtils.safeDelete(zooKeeper,readyNode,-1);
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "ZkCountDownLatch[ Latch Node = "+baseNode+", Current Count = "+getCount()+", total = "+ total+"]";
    }


    
/*-----------------------------------------------------------------------------------------------------------------*/
    /*private helper methods*/

    /*
     * Ensures that this CountDownLatch is in a correct state with respect to other latches which may
     * be open on other nodes.
     *
     * This method first ensures that a given znode exists. The existence of this node is indication that the
     * Latch was fully created. If that node does not exist, then the assumption is that the Latch needs to be
     * instantiated(either another node failed during instantiation, or the latch has never been created), in which
     * case, any znodes which match the LatchPrefix are deleted.
     *
     */
    private void ensureState() {
        ensureNodeExists();
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        //TODO -sf- is there a non-blocking way to do this?
        Lock lock = new ReentrantZkLock(baseNode, zkSessionManager, privileges);
        try {
            lock.lock();
            if(zooKeeper.exists(baseNode+"/countDown-latchReady",false)==null){
                clearState(zooKeeper,latchPrefix);
                zooKeeper.create(baseNode+"/countDown-latchReady",emptyNode,privileges,CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            lock.unlock();
        }
    }



}
