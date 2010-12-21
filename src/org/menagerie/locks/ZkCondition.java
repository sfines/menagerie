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
 *
 */
package org.menagerie.locks;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkPrimitive;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;



/**
 * A ZooKeeper-based implementation of a {@link java.util.concurrent.locks.Condition}.
 * <p><p>
 * This class adheres to the idioms of the {@link java.util.concurrent.locks.Condition} interface wherever possible.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 16-Dec-2010
 *          Time: 20:13:19
 */
final class ZkCondition extends ZkPrimitive implements Condition {
    private static final String conditionPrefix="condition";
    private static final char conditionDelimiter = '-';
    private final ReentrantZkLock distributedLock;

    public ZkCondition(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges, ReentrantZkLock lock) {
        super(baseNode, zkSessionManager, privileges);
        this.distributedLock = lock;
    }

    @Override
    public void await() throws InterruptedException {
        await(Long.MAX_VALUE,TimeUnit.DAYS);
    }

    @Override
    public void awaitUninterruptibly() {
        if(!distributedLock.hasLock())
            throw new IllegalMonitorStateException("await was called without owning the associated lock");

        //put a signal node onto zooKeeper, then wait for it to be deleted
        try {
            ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
            String conditionName = zooKeeper.create(baseNode + "/" + conditionPrefix + conditionDelimiter,
                                                            emptyNode, privileges, CreateMode.EPHEMERAL_SEQUENTIAL);
            //now release the associated zkLock
            distributedLock.unlock();
            while(true){
                localLock.lock();
                try{
                    if(zooKeeper.exists(conditionName,signalWatcher)==null){
                        //we have been signalled, so relock and then return
                        distributedLock.lock();
                        return;
                    }else{
                        condition.awaitUninterruptibly();
                    }
                }finally{
                    localLock.unlock();
                }
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            //this can only be thrown by ZooKeeper utilities, which indicates that a
            //communication error between ZkServers and ZkClients occurred, so we had better throw it
            throw new RuntimeException(e);
        }
    }

    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException {
        if(Thread.interrupted())
            throw new InterruptedException();
        if(!distributedLock.hasLock())
            throw new IllegalMonitorStateException("await was called without owning the associated lock");
        try {
            //release the associated zkLock
            distributedLock.unlock();

            //put a signal node onto zooKeeper, then wait for it to be deleted
            ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
            String conditionName = zooKeeper.create(baseNode + "/" + conditionPrefix + conditionDelimiter,
                                                    emptyNode, privileges, CreateMode.EPHEMERAL_SEQUENTIAL);
            
            long timeLeft = nanosTimeout;
            while(true){
                if(Thread.interrupted()){
                    zooKeeper.delete(conditionName,-1);
                    throw new InterruptedException();
                }
                if(checkTimeout(zooKeeper, conditionName, timeLeft)) return timeLeft;

                long start = System.nanoTime();
                localLock.lock();
                try{
                    long endTime = System.nanoTime();
                    timeLeft -= (endTime-start);
                    if(checkTimeout(zooKeeper, conditionName, timeLeft)) return timeLeft;
                    else if(zooKeeper.exists(conditionName,signalWatcher)==null){
                        //we have been signalled, so relock and then return
                        return timeLeft;
                    }else{
                        timeLeft = condition.awaitNanos(timeLeft);
                    }
                }finally{
                    localLock.unlock();
                }
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } finally{
            distributedLock.lock();
        }
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        return awaitNanos(unit.toNanos(time))>0;
    }

    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
        if(!distributedLock.hasLock())
            throw new IllegalMonitorStateException("await is attempted without first owning the associated lock");

        long timeToWait = deadline.getTime()-System.currentTimeMillis();
        return timeToWait > 0 && await(timeToWait, TimeUnit.MILLISECONDS);
    }

    /**
     * Chooses a party which is waiting on this lock, and signals it.
     * <p>
     * Note:This implementation does NOT check the interrupted status of the current thread, as it prioritizes that
     * a signal ALWAYS be sent, if there is a party waiting for that signal.
     *
     * @see #signal() in java.util.concurrent.locks.Condition
     */
    @Override
    public void signal() {
        if(!distributedLock.hasLock())
            throw new IllegalMonitorStateException("Signal is attempted without first owning the signalling lock");

        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        try {
            List<String> conditionsToSignal = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode, false), conditionPrefix);
            if(conditionsToSignal.size()<=0)return; //no parties to signal

            //delete the lowest numbered waiting party
            ZkUtils.sortBySequence(conditionsToSignal,conditionDelimiter);
            zooKeeper.delete(baseNode+"/"+conditionsToSignal.get(0),-1);

        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void signalAll() {
        if(!distributedLock.hasLock())
            throw new IllegalMonitorStateException("Signal is attempted without first owning the signalling lock");

        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        try {
            List<String> conditionsToSignal = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode, false), conditionPrefix);
            if(conditionsToSignal.size()<=0)return; //no parties to signal

            //notify all waiting conditions in sequence
            ZkUtils.sortBySequence(conditionsToSignal,conditionDelimiter);

            for(String condition:conditionsToSignal){
                zooKeeper.delete(baseNode+"/"+condition,-1);
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    
/*--------------------------------------------------------------------------------------------------------------------*/
    /*private helper methods*/
    private boolean checkTimeout(ZooKeeper zooKeeper, String nodeName, long timeLeft)
                                                                    throws InterruptedException, KeeperException {
        if(timeLeft<=0){
            //timed out
            zooKeeper.delete(nodeName,-1);
            return true;
        }
        return false;
    }
}
