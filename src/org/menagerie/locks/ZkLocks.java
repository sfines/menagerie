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

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkSessionManager;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;



/**
 * Static Utility methods for creating ZooKeeper-base lock objects.
 * <p>
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
