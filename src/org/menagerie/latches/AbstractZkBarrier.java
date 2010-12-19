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
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ConnectionListener;
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
 *          Date: 11-Dec-2010
 *          Time: 11:18:17
 */
abstract class AbstractZkBarrier extends ZkPrimitive implements ConnectionListener {
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
        //attach ourselves as a session listener
        zkSessionManager.addConnectionListener(this);
        boolean barrierReached;
         while(true){
            localLock.lock();
            try {
                List<String> children = ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,this),latchPrefix);

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
        zkSessionManager.removeConnectionListener(this);
        return barrierReached;
    }


    /**
     * Determines the highest sequential count node.
     * <p>
     * This ensures that, once a thread on a node has indicated a countDown, failure of that node (and
     * subsequent removal of its ephemeral nodes) will not keep the CountDownLatch on other nodes from progressing
     * to completion.
     *
     * @param children the children which match the latch prefix
     * @param latchDelimiter the separator between the name of the nodes, and their count parts
     * @return the number of the latest child to be added to the node
     */
    protected long highestChild(List<String> children, char latchDelimiter) {
        if(children.size()<=0)return 0l;

        ZkUtils.sortBySequence(children,latchDelimiter);
        //get the highest sequence element number
        String lastChild = children.get(children.size()-1);
        return Long.parseLong(lastChild.substring(lastChild.indexOf(latchDelimiter)+1));
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
            try{
                zk.delete(baseNode+"/"+latchChild,-1);
            }catch(KeeperException ke){
                //if the node has already been deleted, don't worry about it
                if(ke.code()!=KeeperException.Code.NONODE)
                    throw ke;
            }
        }
    }

    @Override
    public void process(WatchedEvent event) {
        localLock.lock();
        try{
            condition.signalAll();
        }finally{
            localLock.unlock();
        }
    }

    @Override
    public void syncConnected() {
        //pretend like the Watcher was just called to pick up any changes
        this.process(null);
    }

    @Override
    public void expired() {
        //ensure that waiting threads try and re-establish themselves
        this.process(null);
    }
}
