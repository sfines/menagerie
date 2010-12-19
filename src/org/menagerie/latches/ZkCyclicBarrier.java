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
import org.apache.zookeeper.data.Stat;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.locks.ReentrantZkLock;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

/**
 * ZooKeeper-based implementation of a CyclicBarrier.
 * <p>
 * Wherever possible, this implementation is consistent with the semantics of
 * {@link java.util.concurrent.CyclicBarrier}, and adheres to as many of the same constraints as are reasonable
 * to expect.
 * <p>
 * A CyclicBarrier causes <i>all</i> threads which call the {@code await()} methods to wait for the barrier to be completed
 * across all participating nodes. The cyclic nature allows the same instance to be repeatedly used, by calling the
 * {@link #reset()} methods.
 * <p>
 * <h4> Node Failure Considerations</h4>
 * <p>
 * Unlike concurrent {@link java.util.concurrent.CyclicBarrier} implementations, this implementation is sensitive
 * to node failure. In a concurrent world, if one thread fails, then the latch may not proceed, causing limited deadlocks,
 * which may not be ideal, but are generally caused by manageable situations which can be accounted for. In the
 * distributed environment, Node failures may occur at any time, for any number of reasons, some of which cannot be
 * controlled or accounted for. In general, it is better, wherever possible, to allow barriers to proceed even in the
 * face of such node failure scenarios.
 * <p>
 * To account for these scenarios, entering this barrier is considered <i>permanent</i>; even if the node subsequently (
 * in the ZooKeeper ordering) fails, other members of the barrier will see this member as entered, allowing them to
 * proceed.
 * <p>
 * Note, however, that if a node fails <i>before</i> it can enter the barrier, it will leave all parties
 * waiting potentially indefinitely for the barrier to proceed. Only <i>after</i> a party has counted down
 * (in the ZooKeeper-server-ordering) will other parties ignore that failure.
 *
 * @author Scott Fines
 * @version 1.0
 * @see java.util.concurrent.CyclicBarrier
 */
public final class ZkCyclicBarrier extends AbstractZkBarrier {
    private static final char delimiter ='-';
    private static final String barrierPrefix="party";

    /**
     * Creates a new CyclicBarrier, or joins a CyclicBarrier which has been previously created by another party
     * on the same latchNode.
     * <p>
     * This constructor uses Open, Unsafe ACL privileges.
     * <p>
     * This constructor checks to ensure that the barrier is in a good state before returning. If the latchNode was the
     * site of a previously broken barrier, then the Barrier is reset as if the {@link #reset()} method was called.
     * <p>
     * When this constructor returns, the latch is guaranteed to be in a clear, unbroken state and is ready to be
     * used.
     *
     * @param zkSessionManager the ZkSessionManager to use
     * @param barrierNode the node the execute the barrier under
     * @param size the number of elements which must enter the barrier before threads may proceed.
     */
    public ZkCyclicBarrier(long size, ZkSessionManager zkSessionManager,String barrierNode) {
        this(size,zkSessionManager,barrierNode, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    /**
     * Creates a new CyclicBarrier, or joins a CyclicBarrier which has been previously created by another party
     * on the same latchNode.
     * <p>
     * This constructor checks to ensure that the barrier is in a good state before returning. If the latchNode was the
     * site of a previously broken barrier, then the Barrier is reset as if the {@link #reset()} method was called.
     * <p>
     * When this constructor returns, the latch is guaranteed to be in a clear, unbroken state and is ready to be
     * used.
     *
     * @param zkSessionManager the ZkSessionManager to use
     * @param barrierNode the node the execute the barrier under
     * @param privileges the privileges for this latch
     * @param size the number of elements which must enter the barrier before threads may proceed.
     */
    public ZkCyclicBarrier(long size, ZkSessionManager zkSessionManager, String barrierNode, List<ACL> privileges) {
        super(size,barrierNode, zkSessionManager,privileges);

        ensureNodeExists();
        try {
            checkReset();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }



    /**
     * Waits until all parties have been invoked on this barrier.
     * <p>
     * If the current thread is not the last to arrive, then it is disabled for thread-scheduling purposes and lies
     * dormant until one of the following conditions are realized:
     * <ol>
     *      <li>The Last party arrives
     *      <li>Some other thread interrupts the current thread
     *      <li>Some other Thread interrupts one of the other waiting parties
     *      <li>Some other party times out while waiting for the barrier
     *      <li>Some other party invokes {@link #reset()} on this barrier
     * </ol>
     * <p>
     * If the current thread:
     * <ul>
     *      <li> has its interrupted status set on entry to this method; or
     *      <li> is interrupted while waiting
     * </ul>
     *
     * Then the Barrier is broken for all other parties, and an {@link InterruptedException}
     * is thrown on this thread, clearing the current interrupted status.
     * <p>
     * This Barrier is considered <i>broken</i> if one of the following conditions apply:
     * <ol>
     *      <li>Another party times out while waiting for the barrier
     *      <li>Another party invoked {@link #reset()} on this barrier
     *      <li>Another party received notification of a Thread Interruption
     * </ol>
     * <p>
     * If any of these situations occur, a {@link java.util.concurrent.BrokenBarrierException} is thrown.
     *
     * @return the index of this thread with respect to all other parties in the Barrier. If this thread is the
     *          first to arrive, index is {@link #getParties()}-1; the last thread to arrive will return 0
     * @throws InterruptedException if the local thread is interrupted, or if there is a communication error
     *          with ZooKeeper
     * @throws BrokenBarrierException if another party breaks the barrier, or another party calls reset while
     *          waiting for the Barrier to complete
     */
    public long await() throws InterruptedException, BrokenBarrierException {
        try {
            return await(Long.MAX_VALUE,TimeUnit.DAYS);
        } catch (TimeoutException e) {
            //should never happen, since we're waiting for forever. Still, throw it anyway, just in case
            throw new RuntimeException(e);
        }
    }

    /**
     * Waits until all parties have been invoked on this barrier, or until the maximum time has been reached.
     * <p>
     * If the current thread is not the last to arrive, then it is disabled for thread-scheduling purposes and lies
     * dormant until one of the following conditions are realized:
     * <ol>
     *      <li>The Last party arrives
     *      <li>Some other thread interrupts the current thread
     *      <li>Some other Thread interrupts one of the other waiting parties
     *      <li>Some other party times out while waiting for the barrier
     *      <li>Some other party invokes {@link #reset()} on this barrier
     *      <li>This thread reaches its time limit (set by {@code timeout}).
     * </ol>
     * <p>
     * If the current thread:
     * <ul>
     *      <li> has its interrupted status set on entry to this method; or
     *      <li> is interrupted while waiting
     * </ul>
     *
     * Then the Barrier is broken for all other parties, and an {@link InterruptedException}
     * is thrown on this thread, clearing the current interrupted status.
     * <p>
     * This Barrier is considered <i>broken</i> if one of the following conditions apply:
     * <ol>
     *      <li>Another party times out while waiting for the barrier
     *      <li>Another party invoked {@link #reset()} on this barrier
     *      <li>Another party received notification of a Thread Interruption
     * </ol>
     * <p>
     * If any of these situations occur, a {@link java.util.concurrent.BrokenBarrierException} is thrown.
     * <p>
     * If the maximum time is reached, and the barrier has not been completed, the barrier will be broken for
     * all other parties, and a {@link java.util.concurrent.TimeoutException} will be thrown on this thread.
     *
     * @param timeout the maximum amount of time to wait for the barrier to complete
     * @param unit the TimeUnit to use
     * @return the index of this thread with respect to all other parties in the Barrier. If this thread is the
     *          first to arrive, index is {@link #getParties()}-1; the last thread to arrive will return 0
     * @throws InterruptedException if the local thread is interrupted, or if there is a communication error
     *          with ZooKeeper
     * @throws BrokenBarrierException if another party breaks the barrier, or another party calls reset while
     *          waiting for the Barrier to complete
     * @throws java.util.concurrent.TimeoutException if the maximum wait time is exceeded before the Barrier can be
     *          completed.
     */
    public long await(long timeout, TimeUnit unit) throws InterruptedException, BrokenBarrierException, TimeoutException {
        if(Thread.interrupted()){
            breakBarrier("A Party has been interrupted");
            throw new InterruptedException();
        }

        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        String myNode;
        try {
            //ensure that the state of the barrier isn't broken
            if(isBroken()){
                //barrier has been broken
                throw new BrokenBarrierException(new String(zooKeeper.getData(getBrokenPath(),false,new Stat())));
            }
            /*
            This must be Persistent_sequential instead of ephemeral_sequential because, if this thread creates this
            element, then dies before the barrier is entered again, then the next count will be one smaller than it
            should be, potentially causing the Barrier to never reach the total needed, resulting in a distributed
            deadlock.
            */
            myNode = zooKeeper.create(getBarrierBasePath(), emptyNode, privileges, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
            throw new InterruptedException(e.getMessage());
        }
        zkSessionManager.addConnectionListener(connectionListener);
        while(true){
            if(Thread.interrupted()){
                breakBarrier("A Party has been interrupted");
                throw new InterruptedException();
            }
            localLock.lock();
            try {
                if(isBroken()){
                    //barrier has been broken
                    throw new BrokenBarrierException(new String(zooKeeper.getData(getBrokenPath(),false,new Stat())));
                }else if(isReset(zooKeeper)){
                    throw new BrokenBarrierException("The Barrier has been reset");
                }
                List<String> children = ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,signalWatcher),barrierPrefix);
                long count = children.size();
                if(count>=total){
                    //latch has been used, so remove latchReady key
                    if(zooKeeper.exists(getReadyPath(),false)!=null){
                        ZkUtils.safeDelete(zooKeeper,getReadyPath(),-1);
                    }
                    //find this node
                    ZkUtils.sortBySequence(children,delimiter);
                    return children.indexOf(myNode.substring(myNode.lastIndexOf("/")+1));
                }else{
                    boolean alerted = condition.await(timeout,unit);
                    System.out.println("Thread-id="+Thread.currentThread().getName()+", Alerted="+alerted);
                    if(!alerted){
                        breakBarrier("Timeout occurred waiting for barrier to complete");
                        throw new TimeoutException("Timed out waiting for barrier to complete");
                    }
                }
            } catch (KeeperException e) {
                throw new InterruptedException(e.getMessage());
            } finally{
                localLock.unlock();
                zkSessionManager.removeConnectionListener(connectionListener);
            }
        }
    }

    /**
     * Gets the number of parties currently waiting on the Barrier.
     * <p>
     * Note that the returned value constitutes a snapshot of the state of this barrier at the time of invocation--It is
     * possible that the current count of the barrier has changed during the execution of this method.
     *
     * @return the current count of the barrier.
     * @throws RuntimeException wrapping:
     * <ul>
     *  <li> {@link org.apache.zookeeper.KeeperException} if the ZooKeeper Server has trouble with the requests
     *  <li> {@link InterruptedException} if the ZooKeeper client has trouble communicating with the ZooKeeper service
     * </ul>
     */
    public long getNumberWaiting(){
        try {
            return ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,false),barrierPrefix).size();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the number of parties required before this barrier can proceed.
     * <p>
     *
     * @return the number of parties required to trip this barrier
     */
    public long getParties(){
        return total;
    }

    /**
     * Determines whether or not this barrier has been broken.
     *
     * @return true if this barrier is broken.
     */
    public boolean isBroken(){
        try {
            return zkSessionManager.getZooKeeper().exists(getBrokenPath(),false)!=null;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Resets this barrier.
     * <p>
     * If any parties are waiting for this barrier when this method is called,then the barrier will be broken,
     * resulting in a {@link java.util.concurrent.BrokenBarrierException} to be thrown on those parties.
     * <p>
     * Note: It is recommended that only one party call this method at a time, to ensure consistency across the cluster.
     * This may be accomplished using a Leader-election protocol. It may be preferable to simply create a new
     * Barrier instance on all nodes.
     */
    public void reset(){
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        //TODO -sf- is there a non-blocking way to do this?
        Lock lock = new ReentrantZkLock(baseNode, zkSessionManager, privileges);

        try {
            lock.lock();
            doReset(zooKeeper);

        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            lock.unlock();
        }
    }

/*------------------------------------------------------------------------------------------------------------------*/
    /*private helper methods*/

    /*
     * Checks the state of the Barrier, and calls reset if necessary
     */
    private void checkReset() throws KeeperException, InterruptedException {
        //determine if we need to reset the state of the barrier
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        //TODO -sf- is there a non-blocking way to do this?
        Lock lock = new ReentrantZkLock(baseNode, zkSessionManager, privileges);

        try {
            lock.lock();
            if(isBroken()||isReset(zooKeeper)||zooKeeper.exists(getReadyPath(),false)==null){
                //need to reset
                doReset(zooKeeper);
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            lock.unlock();
        }
    }

    private void doReset(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        //put a reset node on the path to let people know it's been reset
        if(zooKeeper.exists(getResetPath(),false)==null){
            try{
                zooKeeper.create(getResetPath(),emptyNode,privileges, CreateMode.EPHEMERAL);
            }catch(KeeperException ke){
                if(ke.code()!=KeeperException.Code.NODEEXISTS)
                    throw ke;
            }
        }
        //remove the latchReady state
        if(zooKeeper.exists(getReadyPath(),false)!=null){
            ZkUtils.safeDelete(zooKeeper,getReadyPath(),-1);
        }

        clearState(zooKeeper,barrierPrefix);
        //remove any broken state setters
        if(isBroken()){
            zooKeeper.delete(getBrokenPath(),-1);
        }
        //remove the reset node, since we're back in a good state
        zooKeeper.delete(getResetPath(),-1);

        //put the good state node in place
        zooKeeper.create(getReadyPath(),emptyNode,privileges,CreateMode.PERSISTENT);
    }

    /*
     * Breaks the barrier for all parties.
     */
    private void breakBarrier(String message) {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName+": Breaking barrier");
        if(!isBroken()){
            System.out.println(threadName+": Barrier not already broken, breaking it now");
            try {
                zkSessionManager.getZooKeeper().create(getBrokenPath(),message.getBytes(),privileges,CreateMode.PERSISTENT);
                System.out.println(threadName+": Barrier broken");
            } catch (KeeperException e) {
                //if the node already exists, someone else broke it first, so don't worry about it
                if(e.code()!=KeeperException.Code.NODEEXISTS)
                    throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private String getBrokenPath() {
        return baseNode+"/"+"cyclicBarrier"+delimiter+BarrierState.BROKEN;
    }

    private String getResetPath(){
        return baseNode+"/"+"cyclicBarrier"+delimiter+BarrierState.RESET;
    }

    private String getReadyPath(){
        return baseNode+"/"+"cyclicBarrier"+delimiter+BarrierState.LATCH_READY;
    }


    private String getBarrierBasePath(){
        return baseNode+"/"+barrierPrefix+delimiter;
    }

    private boolean isReset(ZooKeeper zk) throws InterruptedException, KeeperException {
        return zk.exists(getResetPath(),false)!=null;
    }

    private enum BarrierState{
        LATCH_READY,
        BROKEN,
        RESET
    }

}
