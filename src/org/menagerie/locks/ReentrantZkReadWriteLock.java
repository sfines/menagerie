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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
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
 * This class does not impose a reader or a writer preference ordering for lock access. Instead, all lock acquisition
 * requests are processed in order, as follows:
 *
 * <blockquote>
 *  Acquisition requests contend for entry using a ZooKeeper-ordered arrival policy. When the currently held lock
 * is released, either the longest-waiting single writer will be assigned the write lock, or if there are a group
 * of reader parties waiting longer than all waiting writer parties, then all readers in that group will be assigned
 * the read lock.
 * <p>
 * A party that attempts to acquire a read lock for the first time will block if either the write lock is held, or
 * if there is a waiting writer party. The party will block until the oldest currently waiting writer party has
 * acquired and released the write lock. If a waiting writer abandons its wait, it has been considered equivalent to
 * an acquisition and release of the lock.
 * <p>
 * A party that attempts to acquire a write lock for the first time will block unless both the read and write locks are
 * free. Note that, unlike in the concurrent {@link java.util.concurrent.locks.ReentrantReadWriteLock} version, <i>
 * all</i> methods obey ZooKeeper-ordering to determine when a lock may be acquired.
 * </blockquote>
 *<p>
 * Note: Downgrading a WriteLock to a ReadLock is possible on the same thread. To do so, acquire the write lock, then
 * acquire the read lock on the same thread. Finally, release the write lock. However, upgrading from a ReadLock to a
 * WriteLock is <b>not</b> possible without first releasing the ReadLock.
 * <p>
 * Note also that the write lock provides a {@link java.util.concurrent.locks.Condition}implementation that
 * behaves the same way with respect to the write lock as the {@link java.util.concurrent.locks.Condition}
 * implementation provided by {@link ReentrantZkLock#newCondition()}. This condition may only be used
 * for the WriteLock. The ReadLock does not support {@link Condition}s.
 * <p>
 * Note: This implementation supports only {@code Integer.MAX_VALUE} number of write and read locks. Attempts to
 * exceed this will cause integer overflow, resulting in potentially improper Lock state.
 *
 * @author Scott Fines
 * @version 1.0
 * @see java.util.concurrent.locks.ReentrantReadWriteLock
 */
public final class ReentrantZkReadWriteLock implements ReadWriteLock {
    private static final String readLockPrefix="readLock";
    private static final String writeLockPrefix="writeLock";
    private final ReadLock readLock;
    private final WriteLock writeLock;

    /**
     * Constructs a new ReadWriteLock.
     *
     * @param baseNode the node to lock
     * @param zkSessionManager the session manager to use
     * @param privileges the ZooKeeper privileges to use
     */
    public ReentrantZkReadWriteLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        readLock = new ReadLock(baseNode,zkSessionManager,privileges);
        writeLock = new WriteLock(baseNode,zkSessionManager,privileges);
    }

    /**
     * Creates a new ZooKeeper-based ReadWrite lock with open, unsecured ACL privileges.
     *
     * @param baseNode the node to lock
     * @param zkSessionManager the session manager to use
     */
    public ReentrantZkReadWriteLock(String baseNode, ZkSessionManager zkSessionManager) {
        this(baseNode,zkSessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    /**
     * Gets the read lock associated with this lock.
     *
     * @return the read lock associated with this lock
     */
    public ReadLock readLock(){
        return readLock;
    }

    /**
     * Gets the write lock associated with this lock.
     *
     * @return the write lock associated with this lock.
     */
    public WriteLock writeLock(){
        return writeLock;
    }

    public class ReadLock extends ReentrantZkLock{

        private ReadLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
            super(baseNode, zkSessionManager, privileges);
        }

        @Override
        protected boolean tryAcquireDistributed(ZooKeeper zk, String lockNode, boolean watch) throws KeeperException, InterruptedException {
            List<String> writeLocks = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),writeLockPrefix);
            ZkUtils.sortBySequence(writeLocks,lockDelimiter); 

            //if the writeLock is the one in the lead, then add the ReadLock to the same order and return true
            if(writeLocks.size()>0){
                String leadWriteLock = writeLocks.get(0);
                if((baseNode+"/"+leadWriteLock).equals(ReentrantZkReadWriteLock.this.writeLock.getLockName())){
                    zk.create(getBaseLockPath()+lockDelimiter+ZkUtils.parseSequenceString(writeLocks.get(0),lockDelimiter),emptyNode,privileges, CreateMode.EPHEMERAL);
                    zk.delete(lockNode,-1);
                    return true;
                }
            }

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
                if(watch){
                    stat = zk.exists(baseNode+"/"+ lastWriteLock,signalWatcher);
                }else
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

        private WriteLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
            super(baseNode, zkSessionManager, privileges);
        }

        @Override
        protected boolean tryAcquireDistributed(ZooKeeper zk, String lockNode, boolean watch) throws KeeperException, InterruptedException {
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
