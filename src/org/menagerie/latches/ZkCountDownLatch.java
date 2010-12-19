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
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.locks.ZkLocks;

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
 * Note, however, that these latches are one-use-only tools--if repeated uses of the same instance are required,
 * consider using a {@link ZkCyclicBarrier} instead. However, multiple uses of the
 * same LatchNode <i>is</i> allowed
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
     */
     ZkCountDownLatch(long total,String latchNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        super(total, latchNode, zkSessionManager, privileges);

        ensureState();
    }



    /**
     *  Decrements the count of the latch, releasing all waiting threads in the cluster when the count reaches zero.
     * <p>
     * If the current count of the latch is zero, this has no effect.
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
     * @return the current count of the latch.
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
     * If the current count of the latch is zero, this method returns immediately
     * <p>
     * If the current count of the latch is non-zero, the thread is disabled and lies dormant until one of two
     * things happen:
     * <p>
     *  1.the count reaches zero<br/>
     *  2. The current thread gets interrupted
     * <p>
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
     *
     * @param timeout the maximum time to wait, in {@code unit} units.
     * @param unit the TimeUnit to use
     * @return true if the latch counted down fully before the timeout was reached, false otherwise. 
     * @throws InterruptedException if the current thread is interrupted, or if communication between the ZooKeeper
     * client and server fails in some way
     */
    public boolean await(long timeout, TimeUnit unit)throws InterruptedException{
        return doWait(timeout,unit,latchPrefix);
    }

    /**
     * Closes the latch, signalling that this Latch has been expended.
     * <p>
     * When this method is finished, all latch nodes relating to this latch will be removed, and the
     * state of the latchNode will be ready to accept new latches.
     *
     */
    public void closeLatch() {
        ensureNodeExists();
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        //TODO -sf- is there a non-blocking way to do this?
        Lock lock = ZkLocks.newReentrantLock(zkSessionManager, baseNode, privileges);
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
        Lock lock = ZkLocks.newReentrantLock(zkSessionManager, baseNode, privileges);
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
