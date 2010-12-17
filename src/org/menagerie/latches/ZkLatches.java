package org.menagerie.latches;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkSessionManager;

import java.util.List;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Dec-2010
 *          Time: 13:34:47
 */
public class ZkLatches {

    /*can't instantiate me!*/
    private ZkLatches(){}

    public static ZkCyclicBarrier newCyclicBarrier(long size, String barrierNode, ZkSessionManager sessionManager){
        return new ZkCyclicBarrier(size, sessionManager,barrierNode, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public static ZkCyclicBarrier newCyclicBarrier(long size, String barrierNode, ZkSessionManager sessionManager, List<ACL> privileges){
        return new ZkCyclicBarrier(size, sessionManager,barrierNode, privileges);
    }

    public static ZkCountDownLatch newCountDownLatch(long size, String barrierNode, ZkSessionManager sessionManager){
        return new ZkCountDownLatch(size,barrierNode, sessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public static ZkCountDownLatch newCountDownLatch(long size, String barrierNode, ZkSessionManager sessionManager,List<ACL> privileges){
        return new ZkCountDownLatch(size,barrierNode, sessionManager, privileges);
    }

}
