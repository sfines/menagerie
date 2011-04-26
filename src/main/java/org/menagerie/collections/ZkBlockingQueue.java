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

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.ClusterSafe;
import org.menagerie.Serializer;
import org.menagerie.ZkPrimitive;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.locks.ReentrantZkReadWriteLock;

/**
 * Blocking Queue implementation based on ZooKeeper.
 * <p>
 * This is a full implementation of a BlockingQueue based on ZooKeeper; it supports all the same behaviors
 * as LinkedBlockingQueue.
 * <p>
 * In this implementation, it is possible to specify either bounded or unbounded <i>at construction time</i>. If
 * one of the constructors which don't specify an integer bound are used, then an unbounded queue is instantiated,
 * otherwise, a bounded queue is created.
 * <p>
 * A bounded queue allows only a fixed number of entries to the queue. Attempts to add elements to the queue after that
 * number has been established will <i>block</i> until space becomes free on the queue.
 * <p>
 * An unbounded queue is functionally equivalent to a bounded queue with a bound size of {@code Integer.MAX_VALUE}, but
 * is allowed to be implemented differently for performance reasons. Attempts to add elements to an unbounded queue will
 * always return as soon as the element has been added to the queue.
 * <p>
 * Nodes on the queue are <i>persistent</i>, living in ZooKeeper beyond the length of the session which created the node.
 *
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 15-Jan-2011
 *          Time: 10:51:46
 */
@ClusterSafe
public final class ZkBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private final QueueSync<E> sync;
    private static final String queuePrefix = "queue";
    private static final char queueDelimiter = '-';

    /**
     * Creates a new ZkPrimitive with the correct node information.
     *
     * @param baseNode         the base node to use
     * @param serializer       the serializer to use
     * @param zkSessionManager the session manager to use
     */
    public ZkBlockingQueue(String baseNode, Serializer<E> serializer, ZkSessionManager zkSessionManager) {
        this(baseNode,serializer,zkSessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    /**
     * Creates a new ZkPrimitive with the correct node information.
     *
     * @param baseNode         the base node to use
     * @param serializer       the serializer to use
     * @param zkSessionManager the session manager to use
     * @param privileges       the privileges for this node.
     */
    public ZkBlockingQueue(String baseNode, Serializer<E> serializer, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        sync = new UnboundedSync<E>(baseNode,zkSessionManager,privileges,serializer);
    }

    /**
     * Creates a new ZkPrimitive with the correct node information.
     *
     * @param baseNode         the base node to use
     * @param serializer       the serializer to use
     * @param zkSessionManager the session manager to use
     * @param bound            the maximum size of the queue
     */
    public ZkBlockingQueue(String baseNode, Serializer<E> serializer, ZkSessionManager zkSessionManager, int bound) {
        this(baseNode,serializer,zkSessionManager,ZooDefs.Ids.OPEN_ACL_UNSAFE,bound);
    }

    /**
     * Creates a new ZkPrimitive with the correct node information.
     *
     * @param baseNode         the base node to use
     * @param serializer       the serializer to use
     * @param zkSessionManager the session manager to use
     * @param privileges       the privileges for this node.
     * @param bound            the maximum size of the queue
     */
    public ZkBlockingQueue(String baseNode, Serializer<E> serializer, ZkSessionManager zkSessionManager, List<ACL> privileges, int bound) {
        if(bound<0)
            throw new IllegalArgumentException("Cannot create a queue with a bound less than zero!");
        sync = new BoundedSync<E>(baseNode,bound,zkSessionManager,privileges,serializer);
    }

    @Override
    public Iterator<E> iterator() {
        return sync.iterator();
    }

    @Override
    public int size() {
        return sync.size();
    }

    @Override
    public boolean offer(E e) {
        return sync.offer(e);
    }

    @Override
    public E poll() {
        return sync.poll();
    }

    @Override
    public E element() {
        return sync.element();
    }

    @Override
    public E peek() {
        return sync.peek();
    }

    @Override
    public void put(E e) throws InterruptedException {
        sync.put(e);
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return sync.offer(e,timeout,unit);
    }

    @Override
    public E take() throws InterruptedException {
        return sync.take();
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return sync.poll(timeout,unit);
    }

    @Override
    public int remainingCapacity() {
        return sync.remainingCapacity();
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        if(this.equals(c))
            throw new IllegalArgumentException("Cannot drain queue to itself");
        return sync.drainTo(c);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if(this.equals(c))
            throw new IllegalArgumentException("Cannot drain queue to itself");
        return sync.drainTo(c, maxElements);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ZkBlockingQueue)) return false;

        ZkBlockingQueue that = (ZkBlockingQueue) o;
        if (sync != null ? !sync.equals(that.sync) : that.sync != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return sync != null ? sync.hashCode() : 0;
    }

    private static abstract class QueueSync<E> extends ZkPrimitive{
        protected final Serializer<E> serializer;
        protected final ReadWriteLock headLock;

        /**
         * Creates a new ZkPrimitive with the correct node information.
         *
         * @param baseNode         the base node to use
         * @param zkSessionManager the session manager to use
         * @param privileges       the privileges for this node.
         * @param serializer       the serializer to use to serialize entries
         */
        protected QueueSync(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges, Serializer<E> serializer) {
            super(baseNode, zkSessionManager, privileges);
            this.serializer = serializer;
            headLock = new ReentrantZkReadWriteLock(baseNode,zkSessionManager,privileges);
        }


        int drainTo(Collection<? super E> c) {
            return drainTo(c,Integer.MAX_VALUE);
        }

        int drainTo(Collection<? super E> c, int maxElements) {
            Lock writeLock = headLock.writeLock();
            writeLock.lock();
            try{
                ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
                List<String> entries = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode,false),queuePrefix);
                ZkUtils.sortBySequence(entries,queueDelimiter);
                int removed=0;
                for(String entry:entries){
                    if(removed>=maxElements)
                        break; //we're finished
                    byte[] data = ZkUtils.safeGetData(zooKeeper, baseNode + "/" + entry, false, new Stat());
                    if(data.length>0){
                        ZkUtils.safeDelete(zooKeeper,baseNode+"/"+entry,-1);
                        c.add(serializer.deserialize(data));
                        removed++;
                    }
                }
                return removed;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                writeLock.unlock();
            }
        }

        abstract boolean offer(E e);

        abstract boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException;

        E poll(long timeout, TimeUnit unit) throws InterruptedException {
            if(Thread.interrupted()){
                throw new InterruptedException();
            }
            long timeoutNanos = unit.toNanos(timeout);
            while(true){
                if(Thread.interrupted())
                    throw new InterruptedException();
                else if(timeoutNanos<=0) //we timed out
                    return null;

                //acquire a local lock to allow us to wait if necessary
                long elapsed = tryLockTimed(localLock,timeoutNanos);
                if(elapsed<=0){
                    //didn't get the local lock in time, so return null
                    return null;
                }

                long timeLeft = timeoutNanos-elapsed; //time left in our timeout
                try{
                    ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();

                    //acquire a distributed write-lock to protect us from compound-action troubles
                    Lock writeLock = headLock.writeLock();
                    long distElapsed = tryLockTimed(writeLock, timeLeft);
                    if(distElapsed<0) //couldn't get the distributed lock in time
                        return null;
                    timeLeft = timeLeft - distElapsed;
                    try{
                        E element = getAndDelete(zooKeeper,true);
                        if(element!=null)
                            return element; //we found it!
                    }finally{
                        writeLock.unlock();
                    }
                    //There are no elements on the queue--wait until one becomes available
                    timeoutNanos = condition.awaitNanos(timeLeft);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } finally{
                    localLock.unlock();
                }
            }
        }

        abstract void put(E e) throws InterruptedException;

        abstract int remainingCapacity();

        E take() throws InterruptedException{
            return poll(Long.MAX_VALUE,TimeUnit.DAYS);
        }

        E element(){
            E element = peek();
            if(element==null)
                throw new NoSuchElementException();
            return element;
        }

        E peek(){
            Lock lock = headLock.readLock();
            lock.lock();
            try{
                ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
                List<String> queueEntries = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode, false), queuePrefix);
                ZkUtils.sortBySequence(queueEntries,queueDelimiter);
                //get the first entry's data
                for(String queueEntry:queueEntries){
                    byte[] bytes = ZkUtils.safeGetData(zooKeeper, baseNode + "/" + queueEntry, false, new Stat());
                    if(bytes.length>0){
                        //return this entry
                        return serializer.deserialize(bytes);
                    }
                }
                return null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                lock.unlock();
            }
        }

        E poll() {
            Lock lock = headLock.writeLock();
            lock.lock();
            try{
                return getAndDelete(zkSessionManager.getZooKeeper(),false);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                lock.unlock();
            }
        }

        public Iterator<E> iterator(){
            return new ZkReadWriteIterator<E>(baseNode, serializer, zkSessionManager, privileges, queuePrefix, queueDelimiter, headLock);
        }

        int size() {
            try {
                return ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode, false), queuePrefix).size();
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        protected E getAndDelete(ZooKeeper zooKeeper, boolean watch) throws KeeperException, InterruptedException {
            List<String> queueEntries;
            if(watch)
                queueEntries = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode, signalWatcher), queuePrefix);
            else
                queueEntries = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode, false), queuePrefix);

            ZkUtils.sortBySequence(queueEntries,queueDelimiter);
            //get the first entry's data
            for(String queueEntry:queueEntries){
                byte[] bytes = ZkUtils.safeGetData(zooKeeper, baseNode + "/" + queueEntry, false, new Stat());
                if(bytes.length>0){
                    //delete that entry and return
                    ZkUtils.safeDelete(zooKeeper,baseNode+"/"+queueEntry,-1);
                    return serializer.deserialize(bytes);
                }
            }
            //There are no elements on the queue--return null
            return null;
        }

        protected long tryLockTimed(Lock lock, long timeoutNanos) throws InterruptedException {
            long start = System.nanoTime();
            boolean acquired = lock.tryLock(timeoutNanos, TimeUnit.NANOSECONDS);
            long end = System.nanoTime();
            if(!acquired)return -1l;
            else return (end-start);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof QueueSync)) return false;
            if (!super.equals(o)) return false;

            QueueSync queueSync = (QueueSync) o;

            return queueSync.baseNode.equals(baseNode);
        }

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + (baseNode != null ? baseNode.hashCode() : 0);
            return result;
        }
    }

    private static class UnboundedSync<E> extends QueueSync<E>{
        private final ReadWriteLock headLock;

        /**
         * Creates a new ZkPrimitive with the correct node information.
         *
         * @param baseNode         the base node to use
         * @param zkSessionManager the session manager to use
         * @param privileges       the privileges for this node.
         * @param serializer       the serializer to use
         */
        protected UnboundedSync(String baseNode, ZkSessionManager zkSessionManager,
                                List<ACL> privileges, Serializer<E> serializer) {
            super(baseNode, zkSessionManager, privileges, serializer);
            headLock = new ReentrantZkReadWriteLock(baseNode,zkSessionManager,privileges);
        }

        @Override
        boolean offer(E e) {
            if(e==null) throw new NullPointerException();
            try {
                zkSessionManager.getZooKeeper().create(baseNode+"/"+queuePrefix+queueDelimiter,
                        serializer.serialize(e),privileges, CreateMode.PERSISTENT_SEQUENTIAL);
                return true;
            } catch (KeeperException e1) {
                throw new RuntimeException(e1);
            } catch (InterruptedException e1) {
                throw new RuntimeException(e1);
            }
        }

        @Override
        boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
            /*
            since we are unbounded, we know that we will never block in these methods, so just return offer(e)
            and ignore the timeout stuff
             */
            return offer(e);
        }

        @Override
        void put(E e) {
            offer(e);
        }

        @Override
        int remainingCapacity() {
            return Integer.MAX_VALUE;
        }
    }

    private static class BoundedSync<E> extends QueueSync<E>{
        private final int bound;
        /**
         * Creates a new ZkPrimitive with the correct node information.
         *
         * @param baseNode         the base node to use
         * @param zkSessionManager the session manager to use
         * @param privileges       the privileges for this node.
         * @param serializer       the serializer to use to serialize entries
         */
        protected BoundedSync(String baseNode, int bound, ZkSessionManager zkSessionManager, List<ACL> privileges, Serializer<E> serializer) {
            super(baseNode, zkSessionManager, privileges, serializer);
            this.bound = bound;
        }

        @Override
        boolean offer(E e) {
            if(e==null)
                throw new NullPointerException();
            ZooKeeper zk = zkSessionManager.getZooKeeper();
            try {
                //add the entry to ZooKeeper first
                String myNode = zk.create(baseNode + "/" + queuePrefix + queueDelimiter, serializer.serialize(e), privileges, CreateMode.PERSISTENT_SEQUENTIAL);

                //now determine if your position in line was enough to proceed
                List<String> queueEntries = ZkUtils.filterByPrefix(zk.getChildren(baseNode, false), queuePrefix);
                ZkUtils.sortBySequence(queueEntries,queueDelimiter);

                //find my position in the list
                int myPos = queueEntries.indexOf(myNode.substring(myNode.lastIndexOf("/")+1));
                if(myPos<bound){
                    //we are in the queue, so return happy
                    return true;
                }else{
                    //we didn't make it in the queue, so delete myNode and return false
                    ZkUtils.safeDelete(zk,myNode,-1);
                    return false;
                }
            } catch (KeeperException ke) {
                throw new RuntimeException(ke);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }

        }

        @Override
        boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
            if(e==null)
                throw new NullPointerException();

            ZooKeeper zk = zkSessionManager.getZooKeeper();
            String myNode;
            try {
                //add the entry to ZooKeeper first
                myNode = zk.create(baseNode + "/" + queuePrefix + queueDelimiter, serializer.serialize(e), privileges, CreateMode.PERSISTENT_SEQUENTIAL);
            } catch (KeeperException ke) {
                throw new RuntimeException(ke);
            } catch (InterruptedException ie) {
                throw new RuntimeException(ie);
            }

            //now we wait until either we enter the queue or the timeout occurs
            long timeoutNanos = unit.toNanos(timeout);
            while(true){
                if(Thread.interrupted())
                    throw new InterruptedException();
                localLock.lock();
                try{
                    if(timeoutNanos<=0){
                        //we timed out, so we didn't get into the queue
                        ZkUtils.safeDelete(zk,myNode,-1);
                        return false;
                    }
                    List<String> queueEntries = ZkUtils.filterByPrefix(zk.getChildren(baseNode, false), queuePrefix);
                    ZkUtils.sortBySequence(queueEntries,queueDelimiter);

                    //find my position in the list
                    int myPos = queueEntries.indexOf(myNode.substring(myNode.lastIndexOf("/")+1));
                    if(myPos<bound){
                        //we are in the queue, so return happy
                        return true;
                    }

                    //we need to assign watchers to the list and wait
                    int nextPos = myPos-1;
                    int numWatched=0;
                    while(nextPos>=0&&numWatched<bound){
                        String nextEntry = queueEntries.get(nextPos);
                        if(zk.exists(baseNode+"/"+ nextEntry,signalWatcher)!=null){
                            numWatched++;
                        }
                        nextPos--;
                    }
                    if(numWatched<bound){
                        //enough items were deleted that we can return anyway!
                        return true;
                    }else{
                        //sad, we have to wait to be informed
                        timeoutNanos = condition.awaitNanos(timeoutNanos);
                    }
                } catch (KeeperException ke) {
                    throw new RuntimeException(ke);
                } finally{
                    localLock.unlock();
                }
            }
        }

        @Override
        void put(E e) throws InterruptedException{
            offer(e, Long.MAX_VALUE,TimeUnit.DAYS);
        }

        @Override
        int remainingCapacity() {
            return bound-size();
        }

        @Override
        int size() {
            int actualSize = super.size();
            if(actualSize>bound)return bound;
            else return actualSize;
        }

    }

}
