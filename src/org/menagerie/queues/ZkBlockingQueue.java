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
package org.menagerie.queues;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.*;
import org.menagerie.locks.ReentrantZkReadWriteLock;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 06-Jan-2011
 *          Time: 21:07:52
 */
@Beta
public class ZkBlockingQueue<E> extends ZkPrimitive implements BlockingQueue<E> {
    private static final String queuePrefix = "queue";
    private static final char queueDelimiter = '-';
    private final Serializer<E> serializer;
    private final int capacity;
    private final ReadWriteLock headLock;
    private final ReadWriteLock tailLock;
    private volatile String headNode;
    private volatile String tailNode;


    public ZkBlockingQueue(Serializer<E> serializer,String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        this(Integer.MAX_VALUE,serializer,baseNode,zkSessionManager,privileges);
    }


    public ZkBlockingQueue(int capacity, Serializer<E> serializer,String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        super(baseNode, zkSessionManager, privileges);
        if(serializer==null)
            throw new IllegalArgumentException("No Serializer specified!");
        this.serializer = serializer;

        if(capacity<=0)
            throw new IllegalArgumentException("Cannot create a queue with a negative capacity!");
        this.capacity = capacity;

        this.headLock = new ReentrantZkReadWriteLock(baseNode,zkSessionManager,privileges);
        this.tailLock = new ReentrantZkReadWriteLock(baseNode,zkSessionManager,privileges);
    }

    @Override
    public boolean add(E e) {
        if(offer(e))
            return true;
        else
            throw new IllegalStateException("The queue is full");
    }

    @Override
    public boolean offer(E e) {
        if(e==null) throw new NullPointerException();
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        try {
            if(ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode,false),queuePrefix).size()>=capacity)return false;
            zooKeeper.create(baseNode+"/"+queuePrefix+queueDelimiter,serializer.serialize(e),privileges, CreateMode.PERSISTENT_SEQUENTIAL);
            return true;
        } catch (KeeperException e1) {
            throw new RuntimeException(e1);
        } catch (InterruptedException e1) {
            throw new RuntimeException(e1);
        }
    }

    @Override
    public E remove() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public E poll() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public E element() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public E peek() {
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        try {
            List<String> queueElements = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode,false),queuePrefix);
            ZkUtils.sortBySequence(queueElements,queueDelimiter);
            Lock lock = headLock.readLock();
            lock.lock();
            try{
                for(String queueElement:queueElements){
                    Stat stat;
                    if((stat = zooKeeper.exists(baseNode+"/"+queueElement,false))!=null){
                        return serializer.deserialize(zooKeeper.getData(baseNode+"/"+queueElement,false,stat));
                    }

                }
            }finally{
                lock.unlock();
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public void put(E e) throws InterruptedException {
        if(e==null) throw new NullPointerException();
        while(true){
            Lock writeLock = headLock.writeLock();
            writeLock.lock();
            ZooKeeper zk = zkSessionManager.getZooKeeper();
            try{
                if(ZkUtils.filterByPrefix(zk.getChildren(baseNode,signalWatcher),queuePrefix).size()>=capacity){
                    //must wait to write
                    writeLock.unlock();
                    condition.await();
                }
            } catch (KeeperException e1) {
                throw new RuntimeException(e1);
            } finally{
                writeLock.unlock();
            }
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if(e==null) throw new NullPointerException();
        long timeoutNanos = unit.toNanos(timeout);
        while(true){
            localLock.lock();
            if(timeoutNanos<=0)return false; //couldn't put it in the queue before the time ran out
            ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
            try{
                if(ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode,signalWatcher),queuePrefix).size()>=capacity){
                    //have to allow the queue lock to open up
                    timeoutNanos = condition.awaitNanos(timeoutNanos);
                }else{
                    //we can add the element
                    zooKeeper.create(baseNode+"/"+queuePrefix+queueDelimiter,serializer.serialize(e),privileges,CreateMode.PERSISTENT_SEQUENTIAL);
                    return true;
                }
            } catch (KeeperException e1) {
                throw new RuntimeException(e1);
            } finally{
                localLock.unlock();
            }
        }
    }

    @Override
    public E take() throws InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int remainingCapacity() {
        return (capacity-size());
    }

    @Override
    public boolean remove(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clear() {
        Lock writeLock = headLock.writeLock();
        writeLock.lock();
        try{
            ZooKeeper zk = zkSessionManager.getZooKeeper();
            List<String> queueNodes = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),queuePrefix);
            for(String queueEntry:queueNodes){
                zk.delete(baseNode+"/"+queueEntry,-1);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } finally{
            writeLock.unlock();
        }
    }

    @Override
    public int size() {
        try {
            return ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,false),queuePrefix).size();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean contains(Object o) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Iterator<E> iterator() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Object[] toArray() {
        return new Object[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
