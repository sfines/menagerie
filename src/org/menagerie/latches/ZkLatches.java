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
