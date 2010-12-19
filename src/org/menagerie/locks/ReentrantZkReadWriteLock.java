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
package org.menagerie.locks;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;


/**
 * ZooKeeper-based implementation of a <i>fair</i> Reentrant Read-Write lock.
 * <p>
 * This class adheres, wherever reasonable, to the idioms specified in
 * {@link java.util.concurrent.locks.ReentrantReadWriteLock}.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 15:37:20
 */
public class ReentrantZkReadWriteLock implements ReadWriteLock {
    private static final String readLockPrefix="readLock";
    private static final String writeLockPrefix="writeLock";
    private final ReadLock readLock;
    private final WriteLock writeLock;

    public ReentrantZkReadWriteLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        readLock = new ReadLock(baseNode,zkSessionManager,privileges);
        writeLock = new WriteLock(baseNode,zkSessionManager,privileges);
    }

    public ReadLock readLock(){
        return readLock;
    }

    public WriteLock writeLock(){
        return writeLock;
    }

    public class ReadLock extends ReentrantZkLock{

        protected ReadLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
            super(baseNode, zkSessionManager, privileges);
        }

        @Override
        protected boolean askForLock(ZooKeeper zk, String lockNode, boolean watch) throws KeeperException, InterruptedException {
            List<String> writeLocks = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),writeLockPrefix);
            ZkUtils.sortBySequence(writeLocks,lockDelimiter);

            long mySequenceNumber = ZkUtils.parseSequenceNumber(lockNode,lockDelimiter);
            //get all write Locks which are less than my sequence number
            List<String> aheadWriteLocks = new ArrayList<String>();
            for(String writeLock:writeLocks){
                long lockSeqNbr = ZkUtils.parseSequenceNumber(writeLock, lockDelimiter);
                if(lockSeqNbr <mySequenceNumber){
                    //a write lock ahead of us, so add it in
                    aheadWriteLocks.add(writeLock);
                }
            }
            /*
            since aheadWriteLocks is in sorted order, we know that the last element which is still in ZooKeeper
            is the write lock that we have to wait for
             */
            while(aheadWriteLocks.size()>0){
                String lastWriteLock = aheadWriteLocks.remove(aheadWriteLocks.size() - 1);
                Stat stat;
                if(watch)
                    stat = zk.exists(baseNode+"/"+ lastWriteLock,signalWatcher);
                else
                    stat = zk.exists(baseNode+"/"+ lastWriteLock,false);
                if(stat!=null){
                    //this node still exists, so wait in line behind it
                    return false;
                }
            }
            return true;
        }

        @Override
        protected String getBaseLockPath() {
            return baseNode+"/"+readLockPrefix+lockDelimiter;
        }

        /**
         * Read Locks do not support Conditions, so this throws an UnsupportedOperationException
         *
         * @return nothing
         * @throws UnsupportedOperationException always
         */
        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException("Read Locks do not support conditions");
        }
    }

    public class WriteLock extends ReentrantZkLock{

        protected WriteLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
            super(baseNode, zkSessionManager, privileges);
        }

        @Override
        protected boolean askForLock(ZooKeeper zk, String lockNode, boolean watch) throws KeeperException, InterruptedException {
            List<String> locks = ZkUtils.filterByPrefix(zk.getChildren(baseNode, false),readLockPrefix,writeLockPrefix);
            ZkUtils.sortBySequence(locks,lockDelimiter);

            long mySequenceNumber = ZkUtils.parseSequenceNumber(lockNode,lockDelimiter);
            //get all write Locks which are less than my sequence number
            List<String> aheadLocks = new ArrayList<String>();
            for(String lock: locks){
                if(ZkUtils.parseSequenceNumber(lock,lockDelimiter)<mySequenceNumber){
                    //a write lock ahead of us, so add it in
                    aheadLocks.add(lock);
                }
            }
            /*
            since aheadLocks is in sorted order, we know that the last element which is still in ZooKeeper
            is the lock that we have to wait for
             */
            while(aheadLocks.size()>0){
                String lastReadLock = aheadLocks.remove(aheadLocks.size() - 1);
                Stat stat;
                if(watch)
                    stat = zk.exists(baseNode+"/"+ lastReadLock,signalWatcher);
                else
                    stat = zk.exists(baseNode+"/"+ lastReadLock,false);
                if(stat!=null){
                    //this node still exists, so wait in line behind it
                    return false;
                }
            }
            return true;
        }

        @Override
        protected String getBaseLockPath() {
            return baseNode+"/"+writeLockPrefix+lockDelimiter;
        }
    }
}
