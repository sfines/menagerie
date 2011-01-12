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
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.Beta;
import org.menagerie.Serializer;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.locks.ReentrantZkLock;
import org.menagerie.locks.ReentrantZkReadWriteLock;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 07-Jan-2011
 *          Time: 20:26:19
 */
@Beta
public class ZkHashMap<K,V>  implements ConcurrentMap<K,V> {
    private final ZkSegment<K,V>[] segments;
    private final int segmentShift;
    private final int segmentMask;

    public ZkHashMap(String mapNode, ZkSessionManager zkSessionManager,Serializer<Entry<K,V>> serializer){
        this(mapNode,zkSessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE,serializer);
    }

    public ZkHashMap(String mapNode, ZkSessionManager zkSessionManager,
                     List<ACL> privileges, Serializer<Entry<K,V>> serializer) {
        this(mapNode, zkSessionManager,privileges,serializer,16);
    }

    public ZkHashMap(String baseNode, ZkSessionManager zkSessionManager,
                     List<ACL> privileges, Serializer<Entry<K,V>> serializer, int concurrency) {
        /*
        Shamelessly stolen from the implementation of ConcurrentHashMap
        */
        int sshift = 0;
        int ssize = 1;
        while (ssize < concurrency) {
            ++sshift;
            ssize <<= 1;
        }
        segmentShift = 32 - sshift;
        segmentMask = ssize - 1;
        this.segments = ZkSegment.newArray(ssize);

        //need to build the map in a single, synchronized activity across all members
        Lock mapLock = new ReentrantZkLock(baseNode,zkSessionManager,privileges);
        mapLock.lock();
        try{
            //need to read the data out of zookeeper
            ZooKeeper zk = zkSessionManager.getZooKeeper();
            for(int i=0;i< segments.length;i++){
                try {
                    //attach the first segments to any segments which are already created
                    List<String> zkBuckets = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),"bucket");
                    ZkUtils.sortBySequence(zkBuckets,'-');
                    int numBucketsBuilt = 0;
                    for(int bucketIndex=0;bucketIndex<zkBuckets.size()&&bucketIndex< segments.length;bucketIndex++){
                        numBucketsBuilt++;
                        segments[bucketIndex] = new ZkSegment<K,V>(baseNode+"/"+zkBuckets.get(bucketIndex),
                                serializer,zkSessionManager,privileges);
                    }

                    //create any additional segments as needed
                    while(numBucketsBuilt< segments.length){
                        String bucketNode = ZkUtils.safeCreate(zk, baseNode + "/bucket-",
                                new byte[]{}, privileges, CreateMode.PERSISTENT_SEQUENTIAL);
                        segments[numBucketsBuilt] = new ZkSegment<K,V>(bucketNode,
                                serializer,zkSessionManager,privileges);
                        numBucketsBuilt++;
                    }
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }finally{
            mapLock.unlock();
        }
    }


    @Override
    public V putIfAbsent(K key, V value) {
        if(value==null) throw new NullPointerException();

        int hash = hash(key.hashCode());
        return bucketFor(hash).put(key,hash, value,true);
    }

    @Override
    public boolean remove(Object key, Object value) {
        int hash = hash(key.hashCode());
        return value != null && bucketFor(hash).remove(key, hash, value) != null;
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
        int hash = hash(key.hashCode());
        return bucketFor(hash).replace(key,hash,oldValue,newValue);
    }

    @Override
    public V replace(K key, V value) {
        if (value == null)
            throw new NullPointerException();
        int hash = hash(key.hashCode());
        return bucketFor(hash).replace(key, hash, value);
    }

    @Override
    public int size() {
        int size=0;
        for(ZkSegment<K,V> bucket: segments){
            int oldSize = size;
            size+=bucket.size();
            if(size<oldSize){
                //we've experienced an integer overflow, so return Integer.MAX_VALUE
                return Integer.MAX_VALUE;
            }
        }
        return size;
    }

    @Override
    public boolean isEmpty() {
        for(ZkSegment<K,V> bucket: segments){
            if(!bucket.isEmpty())return false;
        }
        return true;
    }

    @Override
    public boolean containsKey(Object key) {
        int hash = hash(key.hashCode());

        return bucketFor(hash).containsKey(key, hash);
    }

    @Override
    public boolean containsValue(Object value) {
        //acquire all locks
        for(ZkSegment<K,V> bucket: segments){
            bucket.acquireReadLock();
        }
        try{
            for(ZkSegment<K,V> bucket: segments){
                if(bucket.containsValue(value))
                    return true;
            }
            return false;
        }finally{
            for(ZkSegment<K,V> bucket: segments){
                bucket.releaseReadLock();
            }
        }
    }

    @Override
    public V get(Object key) {
        @SuppressWarnings({"unchecked"}) K castKey = (K)key;
        int hash = hash(castKey.hashCode());

        return bucketFor(hash).get(castKey,hash);
    }

    @Override
    public V put(K key, V value) {
        int hash = hash(key.hashCode());

        return bucketFor(hash).put(key,hash,value,false);
    }

    @Override
    public V remove(Object key) {
        int hash = hash(key.hashCode());

        return bucketFor(hash).remove(key,hash,null);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for(Map.Entry<? extends K, ? extends V> entry:m.entrySet()){
            put(entry.getKey(),entry.getValue());
        }
    }

    @Override
    public void clear() {
        for (ZkSegment<K, V> bucket : segments)
            bucket.clear();
    }

    @Override
    public Set<K> keySet() {
        Set<K> keySet = new HashSet<K>(segments.length);
        for(ZkSegment<K,V> bucket: segments){
            keySet.addAll(bucket.keys());
        }
        return Collections.unmodifiableSet(keySet);
    }

    @Override
    public Collection<V> values() {
        Collection<V> values = new LinkedList<V>();
        for(ZkSegment<K,V> bucket: segments){
            values.addAll(bucket.values());
        }
        return Collections.unmodifiableCollection(values);
    }

    /**
     * Gets the entry set for this map.
     * <p>
     * The returned map has weakly-consistent iterators. That is, changes made to the underlying map after the
     * iterator has been created will reflect the state of the map at creation time, and <i>may</i> (but is not
     * guaranteed to) reflect changes to the map made afterwards.
     * <p>
     * The returned map is read-only
     * @return a unmodifiable, Live-view of the entries in this map.
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        
        Set<Entry<K,V>> entrySet = new HashSet<Entry<K,V>>(segments.length);
        for(ZkSegment<K,V> bucket: segments){
            entrySet.addAll(bucket.entries());
        }
        return Collections.unmodifiableSet(entrySet);
    }

    /*
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ZkHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     */
    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    private static final class ZkSegment<T,V>{
        private static final String entryPrefix = "entry";
        private static final char entryDelimiter = '-';
        private final String bucketNode;
        private final ZkSessionManager sessionManager;
        private final List<ACL> nodePrivileges;
        private final ReadWriteLock safety;
        private final Serializer<Entry<T,V>> serializer;

        private ZkSegment(String bucketNode, Serializer<Entry<T,V>> serializer,
                          ZkSessionManager sessionManager, List<ACL> nodePrivileges) {
            this.bucketNode = bucketNode;
            this.sessionManager = sessionManager;
            this.nodePrivileges = nodePrivileges;
            this.serializer = serializer;

            safety = new ReentrantZkReadWriteLock(bucketNode,sessionManager,nodePrivileges);
        }

        V get(Object key, int hash) {
            Lock readLock = safety.readLock();
            readLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false), entryPrefix + "-" + hash);
                for(String match: hashMatches){
                    byte[] data = ZkUtils.safeGetData(zk, bucketNode + "/" + match, false, new Stat());
                    if(data.length>0){
                        Map.Entry<T,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)){
                            return deserializedEntry.getValue();
                        }
                    }
                }
                return null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                readLock.unlock();
            }
        }

        boolean containsKey(Object key, int hash) {
            Lock readLock = safety.readLock();
            readLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false), getHashedPrefix(hash));
                for(String match: hashMatches){
                    Stat exists = zk.exists(bucketNode+"/"+match,false);
                    if(exists!=null){
                        byte[] data = zk.getData(bucketNode+"/"+match,false,exists);
                        Map.Entry<T,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)){
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
                readLock.unlock();
            }
        }

        boolean containsValue(Object value) {
            Lock readLock = safety.readLock();
            readLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false), entryPrefix);
                for(String match: hashMatches){
                    Stat exists = zk.exists(bucketNode+"/"+match,false);
                    if(exists!=null){
                        byte[] data = zk.getData(bucketNode+"/"+match,false,exists);
                        Map.Entry<T,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getValue().equals(value)){
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
                readLock.unlock();
            }
        }

        boolean replace(T key,int hash, V oldValue, V newValue){
            Lock writeLock = safety.writeLock();
            writeLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false),
                        getHashedPrefix(hash));
                for(String match: hashMatches){
                    Stat exists = zk.exists(bucketNode+"/"+match,false);
                    if(exists!=null){
                        byte[] data = zk.getData(bucketNode+"/"+match,false,exists);
                        Map.Entry<T,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)&&deserializedEntry.getValue().equals(oldValue)){
                            //we can set the value
                            zk.setData(bucketNode+"/"+match,serializer.serialize(new AbstractMap.SimpleEntry<T,V>(key,newValue)),-1);
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
                writeLock.unlock();
            }
        }

        V replace(T key, int hash, V newValue){
            Lock writeLock = safety.writeLock();
            writeLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false), getHashedPrefix(hash));
                for(String match: hashMatches){
                    byte[] data = ZkUtils.safeGetData(zk,bucketNode+"/"+match,false,new Stat());
                    if(data.length>0){
                        Map.Entry<T,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)){
                            //we can set the value
                            zk.setData(bucketNode+"/"+match,serializer.serialize(new AbstractMap.SimpleEntry<T,V>(key,newValue)),-1);
                            return newValue;
                        }
                    }
                }
                return null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                writeLock.unlock();
            }
        }

        V put(T key, int hash, V value,boolean onlyIfAbsent){
            Lock writeLock = safety.writeLock();
            writeLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false), entryPrefix + "-" + hash);
                for(String match: hashMatches){
                    byte[] data = ZkUtils.safeGetData(zk,bucketNode+"/"+match,false, new Stat());
                    if(data.length>0){
                        Map.Entry<T,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)){
                            if(onlyIfAbsent){
                                //it isn't absent, so we can't put it
                                return deserializedEntry.getValue();
                            }
                            //we found our match, serialize and set the data
                            zk.setData(bucketNode+"/"+match,serializer.serialize(new AbstractMap.SimpleEntry<T,V>(key,value)),-1);
                            return deserializedEntry.getValue();
                        }
                    }
                }

                //if we make it this far, we didn't find any matches, so just create another entry in this bucket
                zk.create(bucketNode+"/"+ entryPrefix + entryDelimiter +hash+ entryDelimiter,serializer.serialize(new AbstractMap.SimpleEntry<T,V>(key,value)),nodePrivileges, CreateMode.PERSISTENT_SEQUENTIAL);
                return value;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                writeLock.unlock();
            }
        }

        V remove(Object key, int hash, Object oldValue){
            Lock writeLock = safety.writeLock();
            writeLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false), getHashedPrefix(hash));
                for(String match: hashMatches){
                    Stat exists = zk.exists(bucketNode+"/"+match,false);
                    if(exists!=null){
                        byte[] data = zk.getData(bucketNode+"/"+match,false,exists);
                        Map.Entry<T,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)){
                            if(oldValue==null||deserializedEntry.getValue().equals(oldValue)){
                                ZkUtils.safeDelete(zk,bucketNode+"/"+match,-1);
                                return deserializedEntry.getValue();
                            }
                        }
                    }
                }
                return null;
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                writeLock.unlock();
            }
        }

        void clear(){
            Lock writeLock = safety.writeLock();
            writeLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> elements = ZkUtils.filterByPrefix(zk.getChildren(bucketNode,false),entryPrefix);
                for(String entry:elements){
                    ZkUtils.safeDelete(zk,bucketNode+"/"+entry,-1);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                writeLock.unlock();
            }
        }

        int size(){
            try {
                return ZkUtils.filterByPrefix(sessionManager.getZooKeeper().getChildren(bucketNode,false), entryPrefix).size();
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        boolean isEmpty() {
            return size()<=0;
        }

        void acquireReadLock(){
            safety.readLock().lock();
        }

        void releaseReadLock(){
            safety.readLock().unlock();
        }

        Set<T> keys() {
            return new KeySet<T,V>(entries(),safety);
        }

        Collection<V> values(){
            return new ValueCollection<T,V>(entries(),safety);
        }

        ZkSet<Entry<T,V>> entries(){
            return new ZkListSet<Entry<T,V>>(bucketNode,sessionManager,nodePrivileges,serializer,safety){
                @Override
                protected String prefix() {
                    return entryPrefix;
                }

                @Override
                protected char delimiter() {
                    return entryDelimiter;
                }
            };
        }

        @SuppressWarnings({"unchecked"})
        public static <T,V> ZkSegment<T,V>[] newArray(int size) {
            return new ZkSegment[size];
        }

        private String getHashedPrefix(int hash) {
            return entryPrefix + entryDelimiter + hash;
        }
    }

    private static class KeySet<T,V> implements Set<T>{
        private final Set<Entry<T,V>> delegate;
        private final ReadWriteLock safety;

        private KeySet(Set<Entry<T, V>> delegate, ReadWriteLock safety) {
            this.delegate = delegate;
            this.safety = safety;
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            safety.readLock().lock();
            try{
                for(Entry<T,V> entry:delegate){
                    if(entry.getKey().equals(o))return true;
                }
                return false;
            }finally{
                safety.readLock().unlock();
            }
        }

        @Override
        public Iterator<T> iterator() {
            return new KeyIterator<T,V>(delegate.iterator());
        }

        @Override
        public Object[] toArray() {
            safety.readLock().lock();
            try{
                List<T> keys = new ArrayList<T>(delegate.size());
                for(Entry<T,V> entry:delegate){
                    keys.add(entry.getKey());
                }
                return keys.toArray();
            }finally{
                safety.readLock().unlock();
            }
        }

        @Override
        @SuppressWarnings({"unchecked"})
        public <T> T[] toArray(T[] a) {
            Lock readLock = safety.readLock();
            readLock.lock();
            try{
                List keys = new ArrayList(delegate.size());
                for(Entry entry:delegate){
                    keys.add(entry.getKey());
                }
                return (T[]) keys.toArray(a);
            }finally{
                readLock.unlock();
            }
        }

        @Override
        public boolean add(T t) {
            throw new UnsupportedOperationException("Adding elements are not supported by this set");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("Removing elements are not supported by this set");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            for(Object o:c){
                if(!contains(o))return false;
            }
            return true;
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException("Adding elements are not supported by this set");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("Removing elements are not supported by this set");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("Removing elements are not supported by this set");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("Removing elements are not supported by this set");
        }
    }



    private static class ValueCollection<T,V> implements Collection<V>{
        private final Set<Entry<T,V>> delegate;
        private final ReadWriteLock safety;

        private ValueCollection(Set<Entry<T, V>> delegate, ReadWriteLock safety) {
            this.delegate = delegate;
            this.safety = safety;
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean isEmpty() {
            return delegate.isEmpty();
        }

        @Override
        public boolean contains(Object o) {
            safety.readLock().lock();
            try{
                for(Entry<T,V> entry:delegate){
                    if(entry.getKey().equals(o))return true;
                }
                return false;
            }finally{
                safety.readLock().unlock();
            }
        }

        @Override
        public Iterator<V> iterator() {
            return new ValueIterator<T,V>(delegate.iterator());
        }

        @Override
        public Object[] toArray() {
            safety.readLock().lock();
            try{
                List<T> keys = new ArrayList<T>(delegate.size());
                for(Entry<T,V> entry:delegate){
                    keys.add(entry.getKey());
                }
                return keys.toArray();
            }finally{
                safety.readLock().unlock();
            }
        }

        @Override
        @SuppressWarnings({"unchecked"})
        public <V> V[] toArray(V[] a) {
            Lock readLock = safety.readLock();
            readLock.lock();
            try{
                List keys = new ArrayList(delegate.size());
                for(Entry entry:delegate){
                    keys.add(entry.getKey());
                }
                return (V[]) keys.toArray(a);
            }finally{
                readLock.unlock();
            }
        }

        @Override
        public boolean add(V v) {
            throw new UnsupportedOperationException("Adding elements are not supported by this collection");
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException("Removing elements are not supported by this set");
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            Lock lock = safety.readLock();
            lock.lock();
            try{
                for(Object o:c){
                    if(!contains(o))return false;
                }
                return true;
            }finally{
                lock.unlock();
            }
        }

        @Override
        public boolean addAll(Collection<? extends V> c) {
            throw new UnsupportedOperationException("Adding elements are not supported by this collection");
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException("Removing elements are not supported by this set");
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException("Removing elements are not supported by this set");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("Removing elements are not supported by this set");
        }
    }

    private static class KeyIterator<T,V> implements Iterator<T>{
        private final Iterator<Entry<T,V>> entryIterator;

        private KeyIterator(Iterator<Entry<T, V>> entryIterator) {
            this.entryIterator = entryIterator;
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public T next() {
            return entryIterator.next().getKey();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }
    }

    private static class ValueIterator<T,V> implements Iterator<V>{
        private final Iterator<Entry<T,V>> entryIterator;

        private ValueIterator(Iterator<Entry<T, V>> entryIterator) {
            this.entryIterator = entryIterator;
        }

        @Override
        public boolean hasNext() {
            return entryIterator.hasNext();
        }

        @Override
        public V next() {
            return entryIterator.next().getValue();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove is not supported");
        }
    }


    private ZkSegment<K, V> bucketFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }
}
