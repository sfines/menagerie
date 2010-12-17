package org.menagerie.locks;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkSessionManager;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 21-Nov-2010
 *          Time: 13:49:37
 */
public class ZkLocks {

    private ZkLocks(){}

    public static Lock newReentrantLock(ZkSessionManager zkSessionManager,String lockPath){
        return newReentrantLock(zkSessionManager, lockPath, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public static Lock newReentrantLock(ZkSessionManager zkSessionManager,String lockPath, List<ACL> privileges){
        return new ReentrantZkLock(lockPath, zkSessionManager, privileges);
    }

    public static ReadWriteLock newReadWriteLock(ZkSessionManager zkSessionManager, String lockPath){
        return newReadWriteLock(zkSessionManager, lockPath,ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public static ReadWriteLock newReadWriteLock(ZkSessionManager zkSessionManager, String lockPath,List<ACL> privileges){
        return new ReentrantZkReadWriteLock(lockPath, zkSessionManager,privileges);
    }


}
