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
package org.menagerie.latches;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.ZkPrimitive;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A ZooKeeper-based implementation of a fair counting Semaphore.
 * <p>
 * A {@code Semaphore} is a mechanism for allowing a limited number of parties to proceed at any given point in time.
 * To facilitate this, conceptually, a Semaphore maintains a limited number of permits, which it grants to each party
 * which calls {@link #acquire()} in sequence. Once a permit has been granted to a party, that party may proceed with
 * its action, and when that action has finished, the party must return its permit via a call to {@link #release()}
 * before another party may be granted that permit.
 * <p>
 * If no permits are available when a party makes its request, then it is either turned away (if the party calls
 * {@link #tryAcquire()}) or made to wait via blocking (if the party calls {@link #acquire()}).
 * <p>
 * This implementation is conceptually similar to that of {@link java.util.concurrent.Semaphore}, but is distributed
 * and fault-tolerant. If a party acquires a permit, and then subsequently fails, that permit is released back to the
 * Semaphore, allowing another party to proceed.
 * <p>
 * Note that, due to the distributed nature of this semaphore, there are no guarantees that all semaphores in a cluster
 * are required to see the same number of available permits. This makes it possible for a semaphore on Node A to allow,
 * say, three parties access while the same semaphore on Node B allows 5 parties access. Instead, maintaining
 * consistency as to the number of parties which are allowed through each semaphore is the responsibility of the
 * calling code.
 *
 * @author Scott Fines
 * @version 1.1
 * @see java.util.concurrent.Semaphore
 */
public class ZkSemaphore extends ZkPrimitive {
    private volatile int numPermits;
    private static final String permitPrefix = "permit";
    private static final char permitDelimiter = '-';

    /**
     * Creates a new ZkSemaphore with the correct node information.
     *
     * @param numPermits       the number of permits for this semaphore
     * @param baseNode         the base node to use
     * @param zkSessionManager the session manager to use
     * @param privileges       the privileges for this node.
     */
    public ZkSemaphore(int numPermits,String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        super(baseNode, zkSessionManager, privileges);
        this.numPermits = numPermits;
    }

    /**
     * Creates a new ZkSemaphore with the correct node information, and with unsecured privileges
     *
     * @param numPermits       the number of permits for this semaphore
     * @param baseNode         the base node to use
     * @param zkSessionManager the session manager to use
     */
    public ZkSemaphore(int numPermits,String baseNode, ZkSessionManager zkSessionManager) {
        this(numPermits,baseNode, zkSessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    /**
     * Acquires a permit from this semaphore, blocking until one is available or this thread is interrupted.
     * <p>
     * If a permit is immediately available, this method returns immediately, reducing the number of
     * available permits in the semaphore by one.
     * <p>
     * If no permit is available, then the current thread becomes disabled and lies dormant until one of the
     * following occurs:
     * <ul>
     *  <li>Some other party invokes the {@link #release()} method for this semaphore and the current party
     *      is the next to be assigned a permit
     *  <li>Some other party leaves the Semaphore abruptly, as in a node failure scenario.
     *  <li>Some other thread interrupts the current thread
     * </ul>
     *
     * If the current thread:
     *<ul>
     *  <li> has its interrupted status set on entry to this method or
     *  <li> is interrupted while waiting for a permit
     * </ul>
     * then an InterruptedException is thrown and the current thread's interrupted status is cleared.
     *
     * @throws InterruptedException if the current thread is interrupted
     */
    public void acquire() throws InterruptedException{
        tryAcquire(Long.MAX_VALUE,TimeUnit.DAYS);
    }

    /**
     * Acquires a permit from this semaphore, blocking until one is available
     * <p>
     * If a permit is immediately available, this method returns immediately, reducing the number of available
     * permits in the semaphore by one.
     * <p>
     * If no permit is available, this thread becomes disables and lies dormant until some other thread invokes
     * the {@link #release()} method (equivalently, until some other party leaves abruptly, as in a node failure
     * scenario) and the current party is the next to be assigned to a permit.
     * <p>
     * This method will ignore interruptions entirely. If the current Thread is interrupted, this method will continue
     * to wait. When it does return, its interrupt status will be set.
     */
    public void acquireUninterruptibly(){
        acquireUninterruptibly(1);
    }

    /**
     * Acquires a permit from this semaphore <i>only<i/> if it is available at the time of invocation.
     * <p>
     * If a permit is immediately available, this method returns {@code true} immediately, reducing the number of
     * available permits in the semaphore by one.
     * <p>
     * If no permit is available then this method will return immediately with the value {@code false}.
     * <p>
     * Note: Unlike {@link java.util.concurrent.Semaphore#tryAcquire()}, this method <i>will</i> obey ordering policies,
     * and will <i>never</i> acquire a permit at the expense of another party.
     *
     * @return true if a permit has been acquired, false otherwise
     */
    public boolean tryAcquire(){
        return tryAcquire(1);
    }

    /**
     * Acquires a permit from this semaphore if one becomes available within the given waiting time and the
     * current thread has not been interrupted.
     * <p>
     * If a permit is immediately available, this method returns {@code true} immediately, reducing the number of
     * available permits in the semaphore by one.
     * <p>
     * If no permit is available, then the current thread becomes disabled and lies dormant until one of the
     * following occurs:
     * <ul>
     *  <li>Some other party invokes the {@link #release()} method for this semaphore and the current party
     *      is the next to be assigned a permit
     *  <li>Some other party leaves the Semaphore abruptly, as in a node failure scenario.
     *  <li>Some other thread interrupts the current thread
     *  <li>The specified waiting time elapses
     * </ul>
     *
     * If the current thread:
     *<ul>
     *  <li> has its interrupted status set on entry to this method or
     *  <li> is interrupted while waiting for a permit
     * </ul>
     * then an InterruptedException is thrown and the current thread's interrupted status is cleared.
     * <p>
     * If the specified waiting time elapses before a permit may be acquired, then {@code false} is returned. If a
     * permit is obtained before the time has elapsed fully, {@code true} is returned.
     *
     * @param timeout the maximum time to wait for a permit
     * @param unit the time unit to count in 
     * @return true if a permit has been acquired within the specified time, false otherwise
     * @throws InterruptedException if the current thread is interrupted
     */
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException{
        return tryAcquire(1,timeout,unit);
    }

    /**
     * Releases a permit, returning it to the semaphore.
     * <p>
     * Releases a permit, increasing the semaphore's available permits by one. If any parties are waiting to acquire
     * a permit, then the party which has been waiting the longest is selected and given this permit.
     * <p>
     * There is not requirement that a party that releases a permit must have first acquired that permit by calling
     * {@link #acquire()}. This allows parties which have not acquired a specific permit to release a specific permit.
     * However, calling this method without having acquired a permit previously may have dangerous consequences, and is
     * discouraged.
     */
    public void release(){
        release(1);
    }

    /**
     * Acquires the given number of permits from this semaphore, blocking until
     * all are available or this thread is interrupted.
     * <p>
     * If all permits are immediately available, this method returns immediately, reducing the number of
     * available permits in the semaphore by the number requested.
     * <p>
     * If insufficient permits are available, then the current thread becomes disabled and
     * lies dormant until one of the following occurs:
     * <ul>
     *  <li>Some other party invokes the {@link #release()} method for this semaphore and the current party
     *      is the next to be assigned a permit, and sufficient permits are available to satisfy this request
     *  <li>Some other thread interrupts the current thread
     * </ul>
     *
     * If the current thread:
     *<ul>
     *  <li> has its interrupted status set on entry to this method or
     *  <li> is interrupted while waiting for a permit
     * </ul>
     * then an InterruptedException is thrown and the current thread's interrupted status is cleared. Any permits
     * which have been acquired by this method will be released back to the semaphore for reassignment.
     *
     * @param permits the number of permits to acquire
     * @throws InterruptedException if the current thread is interrupted
     */
    public void acquire(int permits) throws InterruptedException{
        tryAcquire(permits,Long.MAX_VALUE,TimeUnit.DAYS);
    }

    /**
     * Acquires the given number of permits from this semaphore, blocking until all is available
     * <p>
     * If all permits are immediately available, this method returns immediately, reducing the number of available
     * permits in the semaphore by the number requested.
     * <p>
     * If insufficient permits are available, this thread becomes disables and lies dormant until some
     * other thread invokes the {@link #release()} method (equivalently, until some other party
     * leaves abruptly, as in a node failure scenario), the current party is the next to be assigned to a permit,
     * and the number of available permits is sufficient to satisfy this request.
     * <p>
     * This method will ignore interruptions entirely. If the current Thread is interrupted, this method will continue
     * to wait. When it does return, its interrupt status will be set.
     *
     * @param permits the number of permits to acquire
     */
    public void acquireUninterruptibly(int permits){
        setConnectionListener();
        ZooKeeper zk = zkSessionManager.getZooKeeper();

        String[] permitNodes = new String[permits];
        try{
            for(int i=0;i<permitNodes.length;i++){
                permitNodes[i] = zk.create(baseNode+"/"+permitPrefix+permitDelimiter,emptyNode,privileges,CreateMode.EPHEMERAL_SEQUENTIAL);
            }
            while(true){
                if(broken)
                    throw new RuntimeException("Connection to ZooKeeper expired while waiting for a permit");

                localLock.lock();
                try{
                    boolean acquiredPermits = false;
                    for(String permitNode:permitNodes){
                        acquiredPermits = acquiredPermits||tryAcquireDistributed(zk,permitNode,true);
                    }
                    if(!acquiredPermits){
                        condition.awaitUninterruptibly();
                    }else
                        return;
                }finally{
                    localLock.unlock();
                }
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            removeConnectionListener();
        }
    }

    /**
     * Acquires the given number of permits from this semaphore <i>only<i/> if sufficient permits are available
     * at the time of invocation.
     * <p>
     * If all permits are immediately available, this method returns {@code true} immediately, reducing the number of
     * available permits in the semaphore by the number of permits requested.
     * <p>
     * If insufficient permits are available then this method will return immediately with the value {@code false}.
     * <p>
     * Note: Unlike {@link java.util.concurrent.Semaphore#tryAcquire()}, this method <i>will</i> obey ordering policies,
     * and will <i>never</i> acquire a permit at the expense of another party.
     *
     * @param permits the number of permits to acquire
     * @return true if a permit has been acquired, false otherwise
     */
    public boolean tryAcquire(int permits){
        ZooKeeper zk = zkSessionManager.getZooKeeper();

        String[] permitNodes = new String[permits];
        try{
            for(int i=0;i<permitNodes.length;i++){
                permitNodes[i] = zk.create(baseNode+"/"+permitPrefix+permitDelimiter,emptyNode,privileges,CreateMode.EPHEMERAL_SEQUENTIAL);
            }
            localLock.lock();
            try{
                boolean acquiredPermits = false;
                for(String permitNode:permitNodes){
                    acquiredPermits = acquiredPermits||tryAcquireDistributed(zk,permitNode,false);
                }
                if(!acquiredPermits){
                    //delete all my nodes to indicate that I'm giving up
                    ZkUtils.safeDeleteAll(zk, -1, permitNodes);
                    return false;
                }
                return true;
            }finally{
                localLock.unlock();
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Acquires the given number of permits from this semaphore if a sufficient number become available within
     * the given waiting time and the current thread has not been interrupted.
     * <p>
     * If enough permits are immediately available, this method returns {@code true} immediately,
     * reducing the number of available permits in the semaphore by the number requested.
     * <p>
     * If insufficient permits are available, then the current thread becomes disabled and lies dormant until one of the
     * following occurs:
     * <ul>
     *  <li>Some other party invokes the {@link #release()} method for this semaphore, the current party
     *      is the next to be assigned a permit, and sufficient permits are available to satisfy this request
     *  <li>Some other thread interrupts the current thread
     *  <li>The specified waiting time elapses
     * </ul>
     *
     * If the current thread:
     *<ul>
     *  <li> has its interrupted status set on entry to this method or
     *  <li> is interrupted while waiting for a permit
     * </ul>
     * then an InterruptedException is thrown and the current thread's interrupted status is cleared.
     * <p>
     * If the specified waiting time elapses before enough permits are acquired, then {@code false} is returned, and
     * any permits which have been granted to this party are released back to the semaphore. If enough permits are
     * obtained before the time has elapsed fully, {@code true} is returned.
     *
     * @param permits the number of permits to acquire
     * @param timeout the maximum time to wait for a permit
     * @param unit the time unit to count in
     * @return true if a permit has been acquired within the specified time, false otherwise
     * @throws InterruptedException if the current thread is interrupted
     */
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit)throws InterruptedException{
        if(Thread.interrupted())
            throw new InterruptedException();

        setConnectionListener();
        ZooKeeper zk = zkSessionManager.getZooKeeper();

        String[] permitNodes = new String[permits];
        try{
            for(int i=0;i<permitNodes.length;i++){
                permitNodes[i] = zk.create(baseNode+"/"+permitPrefix+permitDelimiter,emptyNode,privileges,CreateMode.EPHEMERAL_SEQUENTIAL);
            }
            long timeoutNanos = unit.toNanos(timeout);
            while(true){
                if(broken)
                    throw new InterruptedException("Connection to ZooKeeper expired while waiting for a permit");
                else if(Thread.interrupted()){
                    //delete our permitNodes
                    ZkUtils.safeDeleteAll(zk, -1, permitNodes);
                    throw new InterruptedException();
                }else if(timeoutNanos<=0){
                    //delete our permitNodes
                    ZkUtils.safeDeleteAll(zk, -1, permitNodes);
                    return false;
                }

                localLock.lock();
                try{
                    boolean acquiredPermits = false;
                    for(String permitNode:permitNodes){
                        acquiredPermits = acquiredPermits||tryAcquireDistributed(zk,permitNode,true);
                    }
                    if(!acquiredPermits){
                        timeoutNanos = condition.awaitNanos(timeoutNanos);
                    }else
                        return true;
                }finally{
                    localLock.unlock();
                }
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }finally{
            removeConnectionListener();
        }
    }


    /**
     * Releases the specified number of permits, returning them to the semaphore.
     * <p>
     * Releases the specified number of permits, increasing the semaphore's available permits by the number released.
     * If any parties are waiting to acquire a permit, then they will be allowed to acquire the released permits in
     * the order permits were requested.
     * <p>
     * There is not requirement that a party that releases a permit must have first acquired that permit by calling
     * {@link #acquire()}. This allows parties which have not acquired a specific permit to release a specific permit.
     * However, calling this method without having acquired a permit previously may have dangerous consequences, and is
     * discouraged.
     *
     * @param permits the number of permits to release
     */
    public void release(int permits){
        ZooKeeper zk = zkSessionManager.getZooKeeper();

        int permitsRemaining = permits;
        try {
            List<String> standingPermits = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),permitPrefix);
            ZkUtils.sortBySequence(standingPermits,permitDelimiter);
            for(String permit:standingPermits){
                if(ZkUtils.safeDelete(zk,baseNode+"/"+permit,-1)){
                    permitsRemaining--;
                    if(permitsRemaining<=0)return;
                }
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Gets the number of permits available <i>at the time of invocation</i>
     * <p>
     * Note that size estimates in a distributed environment are necessarily a moving target. This method is typically
     * useful only for debugging and testing purposes, and should not be used for programmatic control flow.
     *
     * @return the number of permits available in this semaphore at the time of invocation
     */
    public int availablePermits(){
        int availablePermits = -getQueueSize();
        if(availablePermits<0)availablePermits=0;
        return availablePermits;
    }

    /**
     * Queries whether any parties are waiting to acquire at the time of invocation.
     * <p>
     * Note that query requests such as this in a distributed environment are necessarily a moving target. This method
     * is typically useful only for debugging and testing purposes, and should not be used for programmatic control flow.
     *
     * @return true if there are parties waiting to acquire a permit
     */
    public final boolean hasQueuedParties(){
        return getQueueLength()>0;
    }

    /**
     * Gets the number of permit requests are outstanding at the time of invocation.
     * <p>
     * Note that size estimates such as this method in a distributed environment are necessarily a moving target. This
     * method is typically useful only for debugging and testing purposes, and should not be used for programmatic
     * control flow.
     *
     * @return the number of parties which are waiting for a permit.
     */
    public final int getQueueLength(){
        int queueLength = getQueueSize();
        if(queueLength<0)queueLength=0;
        return queueLength;
    }

/*---------------------------------------------------------------------------------------------------------------------*/
    /*private helper methods*/

    private int getQueueSize(){
        try {
            return ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode, false), permitPrefix).size() - numPermits;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean tryAcquireDistributed(ZooKeeper zk, String permitNode, boolean watch) throws InterruptedException, KeeperException {
        List<String> permits = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),permitPrefix);
        ZkUtils.sortBySequence(permits,permitDelimiter);

        int permitNumber = ZkUtils.parseSequenceNumber(permitNode,permitDelimiter);
        List<String> aheadPermits = new ArrayList<String>(permits.size());
        for(String permit:permits){
            if(ZkUtils.parseSequenceNumber(permit,permitDelimiter)>=permitNumber)break;
            else{
                aheadPermits.add(permit);
            }
        }
        if(aheadPermits.size()<numPermits){
            //you have a permit, so return
            return true;
        }
        if(watch){
            //need to watch the preceding numPermits nodes
            ZkUtils.sortByReverseSequence(aheadPermits,permitDelimiter);
            int position = aheadPermits.size()-1;
            int numWatchersSet = 0;
            while(position>=0&&numWatchersSet<numPermits){
                Stat exists = zk.exists(baseNode+'/'+aheadPermits.get(0),signalWatcher);
                if(exists!=null){
                    numWatchersSet++;
                }
                position--;
            }
            if(numWatchersSet<numPermits){
                //we got a permit between when we last checked and now, return true!
                return true;
            }
            //have to wait for a watcher to fire
            return false;
        }else
            return false;
    }


}
