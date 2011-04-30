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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
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
 * to node failure. In a concurrent world, it is possible to guarantee that all parties will enter the barrier or will
 * break the barrier, regardless of what else may occur. In a distributed world, external system events such as
 * network partitions and node failures can cause parties to leave a barrier prematurely and uncleanly.
 * <p>
 * To account for these scenarios, this implementation allows the caller to choose whether or not to tolerate external
 * system failures.
 * <p>
 * If the choice is made to be tolerant to external system failures, then barrier entrance is considered
 * <i>permanent</i>; even if the party subsequently (in the ZooKeeper ordering) fails, all other members of the barrier
 * (including members yet to enter) will see that party as entered. This allows the still living members of the barrier
 * to proceed, but may result in the barrier being considered completed when it should not have been.
 * <p>
 * If the choice is made to be intolerant to external system failures, then barrier entrance is
 * considered <i>conditional</i>; if a party enters the barrier, then a system failure event occurs, another member
 * will notice the failure and break the barrier. However, this conditional failure is contingent upon there <i>being</i>
 * another barrier member present when the party fails. This leaves the possibility of a deadlock scenario; if a party
 * enters the barrier and then fails (Zookeeper session timeout) <i>before any other party can
 * enter the barrier</i>, then subsequent parties will not notice the existence and failure of that party, leading
 * the count of parties to always be less than what is necessary to proceed. This scenario requires that the timing
 * between members entering the barrier to be relatively high--there must be at least enough time between the
 * first party and the second party entering for a ZooKeeper session to timeout. If this occurs regularly, then
 * consider increasing the ZooKeeper timeout period for ZooKeeper clients.
 * <p>
 * If even a small risk of deadlocks are unacceptable, and false barrier-completions are acceptable, then instances
 * of this class should call
 * {@link #ZkCyclicBarrier(long, org.menagerie.ZkSessionManager, String, java.util.List, boolean})
 * with {@code tolerateFailures = true}. If a small risk of deadlocks is acceptable, or the ZooKeeper Session timeout
 * is guaranteed to be long enough that the deadlock risk is not present, then instances may call any of the
 * default constructors, or call {@link #ZkCyclicBarrier(long, org.menagerie.ZkSessionManager, String, java.util.List, boolean)}
 * with {@code tolerateFailures = false}.
 *
 * @author Scott Fines
 * @version 1.0
 * @see java.util.concurrent.CyclicBarrier
 */
public final class ZkCyclicBarrier extends AbstractZkBarrier {
    private static final char delimiter ='-';
    private static final String barrierPrefix="party";
    private final CreateMode barrierMode;
    private final AtomicInteger localCount = new AtomicInteger(0);

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
     *  <p>
     * Note: This constructor creates a CyclicBarrier which is not tolerant to party failures. In this mode, a
     * party which enters the Barrier may leave prematurely, due to an external system even such as a
     * network failure; when this happens, the barrier will be broken.
     * <p>
     * However, being so intolerant to failures opens the possibility of deadlocks and timeouts in the following
     * corner case: If Party A enters the barrier <i>before any other party</i>,
     * then leaves the barrier due to some kind of system failure <i>before any other party can arrive</i>,
     * subsequent parties will not see the entrance of Party A, and therefore may potentially timeout or deadlock.
     * For this to happen, the Timings between individual Parties entering the barrier must be high enough to allow
     * time for a ZooKeeper node creation to occur and a ZooKeeper session to expire before the next party may enter.
     * In situations like these, increasing the zooKeeper timeout on individual ZooKeeper clients may fix the situation.
     * When that is not possible, it is possible to set this CyclicBarrier to tolerate party failures, by using the
     * {@link #ZkCyclicBarrier(long, org.menagerie.ZkSessionManager, String, java.util.List, boolean)} constructor
     * instead of this.
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
     *  <p>
     * Note: This constructor creates a CyclicBarrier which is not tolerant to party failures. In this mode, a
     * party which enters the Barrier may leave prematurely, due to an external system even such as a
     * network failure; when this happens, the barrier will be broken.
     * <p>
     * However, being so intolerant to failures opens the possibility of deadlocks and timeouts in the following
     * corner case: If Party A enters the barrier <i>before any other party</i>,
     * then leaves the barrier due to some kind of system failure <i>before any other party can arrive</i>,
     * subsequent parties will not see the entrance of Party A, and therefore may potentially timeout or deadlock.
     * For this to happen, the Timings between individual Parties entering the barrier must be high enough to allow
     * time for a ZooKeeper node creation to occur and a ZooKeeper session to expire before the next party may enter.
     * In situations like these, increasing the zooKeeper timeout on individual ZooKeeper clients may fix the situation.
     * When that is not possible, it is possible to set this CyclicBarrier to tolerate party failures, by using the
     * {@link #ZkCyclicBarrier(long, org.menagerie.ZkSessionManager, String, java.util.List, boolean)} constructor
     * instead of this.
     *
     * @param zkSessionManager the ZkSessionManager to use
     * @param barrierNode the node the execute the barrier under
     * @param privileges the privileges for this latch
     * @param size the number of elements which must enter the barrier before threads may proceed.
     */
    public ZkCyclicBarrier(long size, ZkSessionManager zkSessionManager, String barrierNode, List<ACL> privileges) {
        this(size,zkSessionManager,barrierNode,privileges,false);
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
     * <p>
     * This constructor creates a CyclicBarrier where the caller can choose between node-fault tolerance and algorithmic
     * certainty. If {@code tolerateFailures} is set to true, then once a party has entered this latch, it
     * will remain entered (with respect to the other parties), even if that party subsequently fails.
     * To require that all parties remain alive until the latch has been reached, set {@code tolerateFailures} to false.
     *
     *
     * @param zkSessionManager the ZkSessionManager to use
     * @param barrierNode the node the execute the barrier under
     * @param privileges the privileges for this latch
     * @param tolerateFailures set to {@code true} to allow the Barrier to proceed even if some parties fail after
     *          entering the barrier. Set to false to require all parties to remain alive until the barrier is
     *          completed.
     * @param size the number of elements which must enter the barrier before threads may proceed.
     */
    public ZkCyclicBarrier(long size, ZkSessionManager zkSessionManager, String barrierNode, List<ACL> privileges, boolean tolerateFailures) {
        super(size,barrierNode, zkSessionManager,privileges);

        this.barrierMode = tolerateFailures?CreateMode.PERSISTENT_SEQUENTIAL:CreateMode.EPHEMERAL_SEQUENTIAL;
        ensureNodeExists();
//        try {
//            checkReset();
//            System.out.printf("%s: Created new CyclicBarrier%n",Thread.currentThread().getName());
//        } catch (KeeperException e) {
//            throw new RuntimeException(e);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
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
     *      <li>This barrier is intolerant of party failures, and some other party prematurely leaves the barrier
     *          due to an external system event such as a network failure
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
     *      <li>Another party prematurely leaves the barrier due to an external system event such as a network failure,
     *          and this barrier is set to be intolerant of party failures
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
    public void await() throws InterruptedException, BrokenBarrierException {
        try {
            await(Long.MAX_VALUE,TimeUnit.DAYS);
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
     *      <li>This barrier is intolerant of party failures, and some other party prematurely leaves the barrier
     *          due to an external system event such as a network failure
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
     *      <li>Another party prematurely leaves the barrier due to an external system event such as a network failure,
     *          and this barrier is set to be intolerant of party failures
     *      <li>Another party received notification of a Thread Interruption
     * </ol>
     * <p>
     * If any of these situations occur, a {@link java.util.concurrent.BrokenBarrierException} is thrown.
     * <p>
     * If the maximum time is reached, and the barrier has not been completed, the barrier will be broken for
     * all other parties, and a {@link java.util.concurrent.TimeoutException} will be thrown on this thread.
     *
     * <p>Note that, unlike {@link java.util.concurrent.CyclicBarrier} in the standarad JDK, this method
     * does <em>not</em> return a count of this thread's position in the barrier. This is because it is essentially
     * impossible to do so accurately.
     *
     * @param timeout the maximum amount of time to wait for the barrier to complete
     * @param unit the TimeUnit to use
     * @throws InterruptedException if the local thread is interrupted, or if there is a communication error
     *          with ZooKeeper
     * @throws BrokenBarrierException if another party breaks the barrier, or another party calls reset while
     *          waiting for the Barrier to complete
     * @throws java.util.concurrent.TimeoutException if the maximum wait time is exceeded before the Barrier can be
     *          completed.
     */
    public void await(long timeout, TimeUnit unit) throws InterruptedException, BrokenBarrierException, TimeoutException {
        long timeLeftNanos = unit.toNanos(timeout);
        //add my node to the collection

        String myNode=null;
        try{
            myNode = zkSessionManager.getZooKeeper().create(getBarrierBasePath(),emptyNode,privileges,CreateMode.EPHEMERAL_SEQUENTIAL);

            while(timeLeftNanos>0){
                long start = System.nanoTime();
                boolean acquired = localLock.tryLock(timeLeftNanos, TimeUnit.NANOSECONDS);
                long end = System.nanoTime();
                timeLeftNanos-=(end-start);
                if(!acquired||timeLeftNanos<0) throw new TimeoutException();
                try{
                    if(isCleared()){
                        //the Barrier has been cleared, so we are done
                        return;
                    }else{
                        ZooKeeper zk = zkSessionManager.getZooKeeper();
                        List<String> barrierChildren = ZkUtils.filterByPrefix(zk.getChildren(baseNode, signalWatcher), barrierPrefix);
                        int currentChildren= barrierChildren.size();
                        if(currentChildren>=total){
                            //we've finished, so let's clear and return
                            ZkUtils.safeCreate(zk,getClearedPath(),emptyNode,privileges,CreateMode.PERSISTENT);
                            return;
                        }
                        else if(localCount.get()>barrierChildren.size())throw new BrokenBarrierException();
                        else{
                            localCount.set(barrierChildren.size());
                            /*
                            we know that the barrierChildren's size is at least as large as the local count,
                            but it isn't large enough to finish yet.
                            so let's wait until we receive word that something has changed
                            */
                            timeLeftNanos=condition.awaitNanos(timeLeftNanos);
                        }
                    }
                }finally{
                    localLock.unlock();
                }
            }
            throw new TimeoutException();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } finally{
            //delete myNode
            if(myNode!=null){
                try{
                    ZkUtils.safeDelete(zkSessionManager.getZooKeeper(),myNode,-1);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
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

    public boolean isCleared() throws InterruptedException, KeeperException {
        return zkSessionManager.getZooKeeper().exists(getClearedPath(),false)!=null;
    }
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

            if(!isCleared()&&(isBroken()||isReset(zooKeeper)||zooKeeper.exists(getReadyPath(),false)==null)){
                //need to reset
                doReset(zooKeeper);
            }else{
                //need to set the initial state of the localCounter
                localCount.set(ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,false),barrierPrefix).size());
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            lock.unlock();
        }
    }

    /*Breaks the barrier if the current thread is interrupted, clearing the Thread's interrupted status in the
        process
     */
    private void checkAndBreakIfInterrupted() throws InterruptedException {
        if(Thread.interrupted()){
            breakBarrier("A Party has been interrupted");
            throw new InterruptedException();
        }
    }

    private void doReset(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        //put a reset node on the path to let people know it's been reset
        if(zooKeeper.exists(getResetPath(),false)==null){
            ZkUtils.safeCreate(zooKeeper,getResetPath(),emptyNode,privileges,CreateMode.EPHEMERAL);
        }
        //remove the latchReady state
        if(zooKeeper.exists(getReadyPath(),false)!=null){
            ZkUtils.safeDelete(zooKeeper,getReadyPath(),-1);
        }

        clearState(zooKeeper,barrierPrefix);
        
        //remove any broken state setters
        if(isBroken()){
            ZkUtils.safeDelete(zooKeeper,getBrokenPath(),-1);
        }
        //remove the reset node, since we're back in a good state
        ZkUtils.safeDelete(zooKeeper,getBrokenPath(),-1);

        //delete the reset node
        ZkUtils.safeDelete(zooKeeper,getResetPath(),-1);
        //put the good state node in place
        ZkUtils.safeCreate(zooKeeper,getReadyPath(),emptyNode,privileges,CreateMode.PERSISTENT);
    }

    /*
     * Breaks the barrier for all parties.
     */
    private void breakBarrier(String message) {
        if(!isBroken()){
            try {
                zkSessionManager.getZooKeeper().create(getBrokenPath(),message.getBytes(),privileges,CreateMode.PERSISTENT);
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

    private String getClearedPath(){
        return baseNode+"/"+"cyclicBarrier"+delimiter+BarrierState.CLEARED;
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
        CLEARED,
        RESET
    }

}
