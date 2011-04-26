package org.menagerie;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

/**
 * @author Scott Fines
 *         Date: Apr 25, 2011
 *         Time: 1:55:57 PM
 */
public interface ZkCommand<T> {

    public T execute(ZooKeeper zk) throws KeeperException,InterruptedException;
}
