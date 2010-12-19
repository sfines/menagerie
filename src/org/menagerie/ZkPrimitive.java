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
package org.menagerie;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;



/**
 * Base class for ZooKeeper client primitives.
 * <p>
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 09-Dec-2010
 *          Time: 20:10:01
 */
public class ZkPrimitive {
    protected static final byte[] emptyNode = new byte[]{};
    protected final ZkSessionManager zkSessionManager;
    protected final String baseNode;
    protected final List<ACL> privileges;
    protected final Lock localLock;
    protected final Condition condition;
    protected volatile boolean broken=false;
    protected final ConnectionListener connectionListener = new PrimitiveConnectionListener();
    protected final Watcher signalWatcher;

    protected ZkPrimitive(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        this(baseNode,zkSessionManager,privileges,new ReentrantLock(true));
    }

    protected ZkPrimitive(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges, Lock localLock){
        if(baseNode==null)
            throw new NullPointerException("No base node specified!");
        this.baseNode = baseNode;
        this.zkSessionManager = zkSessionManager;
        this.privileges = privileges;
        this.localLock = localLock;
        condition = this.localLock.newCondition();
        signalWatcher = new SignallingWatcher(this);
        ensureNodeExists();
    }

    /**
     * Ensures that the base node exists in ZooKeeper.
     * <p>
     * Note: This method does NOT create elements recursively--if the base node is a subnode of a
     * node which doesn't exist, a NoNode Exception will be thrown.
     *
     * @throws RuntimeException wrapping a KeeperException if something goes wrong communicating with the ZooKeeper server
     *         RuntimeException wrapping an InterruptedException if something goes wrong communicating with the ZooKeeper
     *                                  Server.
     */
    protected final void ensureNodeExists(){
        try {
            ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();

            Stat stat = zooKeeper.exists(baseNode, false);
            if(stat==null){
                zooKeeper.create(baseNode,emptyNode,privileges, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o.getClass()!=this.getClass()) return false;

        ZkPrimitive that = (ZkPrimitive) o;

        return baseNode.equals(that.baseNode);
    }

    @Override
    public int hashCode() {
        return baseNode.hashCode();
    }

    protected void setConnectionListener(){
        zkSessionManager.addConnectionListener(connectionListener);
    }

    protected void removeConnectionListener(){
        zkSessionManager.removeConnectionListener(connectionListener);
    }

    protected void notifyParties(){
        localLock.lock();
        try{
            condition.signalAll();
        }finally{
            localLock.unlock();
        }
    }

    protected class PrimitiveConnectionListener implements ConnectionListener{

        @Override
        public void syncConnected() {
            //we had to connect to another server, and this may have taken time, causing us to miss our watcher, so let's
            //signal everyone locally and see what we get.
            notifyParties();
        }

        @Override
        public void expired() {
            //indicate that this lock is broken, and alert all waiting threads to throw an Exception
            broken=true;
            notifyParties();
        }
    }

    private static class SignallingWatcher implements Watcher{
        private final ZkPrimitive primitive;

        private SignallingWatcher(ZkPrimitive primitive) {
            this.primitive = primitive;
        }

        @Override
        public void process(WatchedEvent event) {
            primitive.notifyParties();
        }
    }
}
