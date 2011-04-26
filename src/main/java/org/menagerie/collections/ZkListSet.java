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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.*;
import org.menagerie.locks.ReentrantZkReadWriteLock;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * Distributed, Concurrent set based on a List idiom.
 * <p>
 * This implementation uses an internal List-type structure based on ZooKeeper to hold elements. This implementation
 * is functionally equivalent to an implementation of a Distributed, synchronized List which only supports a
 * {@code putIfAbsent} behavior.
 * <p>
 * Note: This implementation relies upon global, synchronous locking on the baseNode. Therefore, this implementation
 * is the distributed equivalent of {@code java.util.Collections.synchronizedSet(java.util.Set)};
 * <p>
 * WARNING: This is <i>not</i> the most efficient implementation possible of a ZooKeeper Set. In the worst case, a traversal
 * of <i>all</i> the elements in this set must occur for a put, get, or remove operation to occur.
 *
 * @author Scott Fines
 * @version 1.0
 * @param <T> the type of entities to be stored in the set
 *          Date: 09-Jan-2011
 *          Time: 20:01:49
 */
@Beta
@ClusterSafe
public class ZkListSet<T> implements Set<T> {
    private final String baseNode;
    private final ZkSessionManager sessionManager;
    private final List<ACL> privileges;
    private final Serializer<T> serializer;
    private final ReadWriteLock safety;

    public ZkListSet(String baseNode, ZkSessionManager sessionManager, List<ACL> privileges, Serializer<T> serializer){
        this(baseNode,sessionManager,privileges,serializer,new ReentrantZkReadWriteLock(baseNode,sessionManager,privileges));
    }

    ZkListSet(String baseNode, ZkSessionManager sessionManager,List<ACL> privileges, Serializer<T> serializer, ReadWriteLock lock){
        this.baseNode = baseNode;
        this.sessionManager = sessionManager;
        this.privileges = privileges;
        this.serializer = serializer;

        safety = lock;
    }

    @Override
    public int size() {
        try {
            return ZkUtils.filterByPrefix(sessionManager.getZooKeeper().getChildren(baseNode,false),prefix()).size();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isEmpty() {
        try {
            return ZkUtils.filterByPrefix(sessionManager.getZooKeeper().getChildren(baseNode,false),prefix()).size()<=0;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean contains(Object o) {
        acquireReadLock();
        try{
            ZooKeeper zk = sessionManager.getZooKeeper();
            List<String> children = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),prefix());
            for(String child:children){
                byte[] data = ZkUtils.safeGetData(zk,baseNode+"/"+child,false,new Stat());
                if(data.length>0){
                    if(serializer.deserialize(data).equals(o))
                        return true;
                }
            }
            return false;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } finally{
            releaseReadLock();
        }
    }

    @Override
    public Iterator<T> iterator() {
        return new ZkReadWriteIterator<T>(baseNode, serializer, sessionManager, privileges, prefix(), delimiter(),safety);
    }

    @Override
    public Object[] toArray() {
        try {
            List<T> actualObjects = toLocalList();
            return actualObjects.toArray();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings({"SuspiciousToArrayCall"})
    public <T> T[] toArray(T[] a) {
        try {
            return toLocalList().toArray(a);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean add(T t) {
        acquireWriteLock();
        try{
            ZooKeeper zk = sessionManager.getZooKeeper();
            List<String> children = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),prefix());
            for(String child:children){
                byte[] data = ZkUtils.safeGetData(zk,baseNode+"/"+child,false,new Stat());
                if(data.length>0){
                    if(serializer.deserialize(data).equals(t))
                        return false;
                }
            }
            zk.create(baseNode+"/"+prefix()+delimiter(),serializer.serialize(t),privileges, CreateMode.PERSISTENT_SEQUENTIAL);
            return true;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } finally{
            releaseWriteLock();
        }
    }

    @Override
    public boolean remove(Object o) {
        acquireWriteLock();
        try{
            ZooKeeper zk = sessionManager.getZooKeeper();
            List<String> children = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),prefix());
            for(String child:children){
                byte[] data = ZkUtils.safeGetData(zk,baseNode+"/"+child,false,new Stat());
                if(data.length>0){
                    if(serializer.deserialize(data).equals(o)){
                        ZkUtils.safeDelete(zk,baseNode+"/"+child,-1);
                        return true;
                    }

                }
            }
            return false;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } finally{
            releaseWriteLock();
        }
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        acquireReadLock();
        try{
            for(Object o : c){
                if(!contains(o))
                    return false;
            }
        }finally{
            releaseReadLock();
        }
        return true;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        acquireWriteLock();
        try{
            boolean changed = false;
            for(T element:c){
                boolean nextChange = add(element);
                changed = changed || nextChange;
            }
            return changed;
        }finally{
            releaseWriteLock();
        }
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        acquireWriteLock();
        try{
            boolean changed= false;
            ZooKeeper zk = sessionManager.getZooKeeper();
            for(String child:ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),prefix())){
                byte[] data = ZkUtils.safeGetData(zk,baseNode+"/"+child,false, new Stat());
                if(data.length>0){
                    if(!c.contains(serializer.deserialize(data))){
                        boolean nextChange = ZkUtils.safeDelete(zk, baseNode + "/" + child, -1);
                        changed = changed || nextChange;
                    }
                }
            }
            return changed;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } finally{
            releaseWriteLock();
        }
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        acquireWriteLock();
        try{
            boolean changed = false;
            for(Object o: c){
                boolean nextChange = remove(o);
                changed = changed || nextChange;
            }
            return changed;
        }finally{
            releaseWriteLock();
        }
    }

    @Override
    public void clear() {
        acquireWriteLock();
        try{
            ZooKeeper zk = sessionManager.getZooKeeper();
            for(String child:ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),prefix())){
                ZkUtils.safeDelete(zk,baseNode+"/"+child,-1);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } finally{
            releaseWriteLock();
        }
    }

    protected String prefix(){
        return "setEntry";
    }

    protected char delimiter(){
        return '-';
    }

    private List<T> toLocalList() throws InterruptedException, KeeperException {
        acquireReadLock();
        try{
            ZooKeeper zk = sessionManager.getZooKeeper();
            List<String> children = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),prefix());
            List<T> localList = new ArrayList<T>(children.size());
            for(String child:children){
                byte[] data = ZkUtils.safeGetData(zk, baseNode + "/" + child, false, new Stat());
                if(data.length>0)
                    localList.add(serializer.deserialize(data));
            }
            return localList;
        }finally{
            releaseReadLock();
        }
    }

    private void releaseReadLock() {
        safety.readLock().unlock();
    }

    private void acquireReadLock() {
        safety.readLock().lock();
    }

    private void acquireWriteLock(){
        safety.writeLock().lock();
    }

    private void releaseWriteLock(){
        safety.writeLock().unlock();
    }


}
