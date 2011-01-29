/*
 * Copyright 2010 Scott Fines
 * <p>
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
package org.menagerie.collections;

import org.apache.zookeeper.data.ACL;
import org.menagerie.Beta;
import org.menagerie.Serializer;
import org.menagerie.ZkSessionManager;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * ZooKeeper iterator which uses a Read-Write lock to enforce ZooKeeper-based synchronization.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 20-Jan-2011
 *          Time: 08:31:16
 */
final class ZkReadWriteIterator<E> extends ZkIterator<E>{
    private final ReadWriteLock safety;

    public ZkReadWriteIterator(String baseNode,
                               Serializer<E> eSerializer,
                               ZkSessionManager zkSessionManager,
                               List<ACL> privileges, String iteratorPrefix,
                               char iteratorDelimiter, ReadWriteLock safety) {
        super(baseNode, eSerializer, zkSessionManager, privileges, iteratorPrefix, iteratorDelimiter);
        this.safety = safety;
    }

    @Override
    public boolean hasNext() {
        Lock readLock = safety.readLock();
        readLock.lock();
        try{
            return super.hasNext();
        }finally{
            readLock.unlock();
        }
    }


    @Override
    public void remove() {
        Lock writeLock = safety.writeLock();
        writeLock.lock();
        try{
            super.remove();
        }finally{
            writeLock.unlock();
        }
    }
}
