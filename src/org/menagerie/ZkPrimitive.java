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
public class ZkPrimitive implements Watcher {
    protected static final byte[] emptyNode = new byte[]{};
    protected final ZkSessionManager zkSessionManager;
    protected final String baseNode;
    protected final List<ACL> privileges;
    protected final Lock localLock = new ReentrantLock();
    protected final Condition condition = localLock.newCondition();

    protected ZkPrimitive(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        if(baseNode==null)
            throw new NullPointerException("No base node specified!");
        this.baseNode = baseNode;
        this.zkSessionManager = zkSessionManager;
        this.privileges = privileges;
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
    public void process(WatchedEvent event) {
        localLock.lock();
        try{
            condition.signalAll();
        }finally{
            localLock.unlock();
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
}
