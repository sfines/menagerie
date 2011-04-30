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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkPrimitive;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Abstract Barrier implementation, based on ZooKeeper
 *
 * @author Scott Fines
 * @version 1.0
 */
abstract class AbstractZkBarrier extends ZkPrimitive {
    /**
     * The total number of parties which must join this barrier before waiting parties may proceed.
     */
    protected final long total;

    protected AbstractZkBarrier(long total, String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        super(baseNode, zkSessionManager, privileges);
        this.total = total;
    }

    /**
     * Causes the current thread to wait until one of the following conditions are met:
     * <p>
     *  1. All required entities join the barrier
     *  2. The Current thread is interrupted
     *  3. The specified timeout is reached.
     * <p>
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit to wait in
     * @param latchPrefix the prefix for latch nodes to use
     * @return true if the barrier was successfully reached before the timeout was reached, false otherwise.
     * @throws InterruptedException if the current thread is interrupted, or if there is communication trouble
     *          between the ZooKeeper service and this client.
     */
    protected final boolean doWait(long timeout, TimeUnit unit, String latchPrefix) throws InterruptedException{
        if(Thread.interrupted())
            throw new InterruptedException();
        //attach ourselves as a session listener
        zkSessionManager.addConnectionListener(connectionListener);
        boolean barrierReached;
        while(true){
            if(Thread.interrupted())
                throw new InterruptedException();
            localLock.lock();
            try {
                List<String> children = ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,signalWatcher),latchPrefix);

                long count = children.size();
                if(count>=total){
                    barrierReached=true;
                    break;
                }else{
                    boolean alerted = condition.await(timeout,unit);
                    if(!alerted){
                        barrierReached=false;
                        break;
                    }
                }
            } catch (KeeperException e) {
                throw new InterruptedException(e.getMessage());
            }finally{
                localLock.unlock();
            }
        }
        zkSessionManager.removeConnectionListener(connectionListener);
        return barrierReached;
    }

    /**
     * Clears the state of the Barrier.
     * <p>
     * When this method returns,the Barrier is fully reset.
     * <p>
     * NOTE: This method does NOT handle its own locking--it is the responsibility of the caller to ensure
     * that the proper locking semantics are called.
     *
     * @param zk the ZooKeeper instance to use
     * @param latchPrefix the latchPrefix to remove
     * @throws KeeperException if there is trouble with the ZooKeeper Service(e.g. A ConnectionLoss, BadArguments, NoAuth, etc)
     * @throws InterruptedException if there is communication trouble with the ZooKeeper Service in mid-operation.
     */
    protected final void clearState(ZooKeeper zk, String latchPrefix) throws KeeperException, InterruptedException{
        List<String> latchChildren = ZkUtils.filterByPrefix(zk.getChildren(baseNode, false), latchPrefix);

        //clear out any old data
        for(String latchChild:latchChildren){
            ZkUtils.safeDelete(zk,baseNode+"/"+latchChild,-1);
        }
    }
}
