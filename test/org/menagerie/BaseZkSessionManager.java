package org.menagerie;

import org.apache.zookeeper.ZooKeeper;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 08:54:27
 */
public class BaseZkSessionManager implements ZkSessionManager {
    private final ZooKeeper zk;

    public BaseZkSessionManager(ZooKeeper zk) {
        this.zk = zk;
    }

    @Override
    public ZooKeeper getZooKeeper() {
        return zk;
    }

    @Override
    public void closeSession() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        //default no-op
    }

    @Override
    public void removeConnectionListener(ConnectionListener listener) {
        //default no-op
    }
}
