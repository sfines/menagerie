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
import org.menagerie.*;
import org.menagerie.locks.ReentrantZkLock;
import org.menagerie.locks.ReentrantZkReadWriteLock;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * ZooKeeper-based implementation of a Concurrent HashMap.
 * <p>
 * This implementation is a ZooKeeper-based equivalent to {@link java.util.concurrent.ConcurrentHashMap}, with similar
 * functional characteristics.
 * <p>
 * This implementation is a shared-access data-structure; Any number of readers are allowed concurrently, but only
 * a fixed number of writers. The number of concurrent writers can be configured by using the
 * {@link #ZkHashMap(String, org.menagerie.ZkSessionManager, java.util.List, org.menagerie.Serializer, int)} constructor.
 * By default, up to 16 writers are allowed concurrently. Note, however, that just because up to 16 writers may be allowed,
 * when two writers are attempting to modify the same data concurrently, contention will result, reducing the realistic
 * number of writers concurrently allowed. Unlike in {@link java.util.concurrent.ConcurrentHashMap}, space is not a
 * realistic concern governing the correct concurrency level in this implementation. Using a high concurrency level
 * may result in less contention among writers, but will slow down operations which iterate over the collection, as
 * more context switches may be required. Using a very low concurrency level, however, will result in higher contention
 * for writers. It is generally preferred, but not required, that a power of 2 be chosen as the concurrency level.
 * <p>
 * It is important to note that setting this concurrency level flag asymmetrically over multiple ZooKeeper clients
 * may result in data which is not accessible to some clients, while being accessible to others. In effect, if this
 * concurrency level is set apart from the default value of 16, be sure to set it symmetrically across all ZooKeeper
 * clients, or risk unexpected data loss scenarios.
 *
 * <p>Note: This implementation does NOT implement {@link java.io.Serializable}, since it stores its entries
 * in a pre-serialized format in ZooKeeper, rather than locally.
 * 
 * <p>
 * This class does <i>not</i> allow <tt>null</tt> to be used as a key or a value.
 *
 * @author Scott Fines
 * @version 1.0
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 *          Date: 07-Jan-2011
 *          Time: 20:26:19
 */
@ClusterSafe
public final class ZkHashMap<K,V>  implements ConcurrentMap<K,V> {
    private final ZkSegment<K,V>[] segments;
    private final int segmentShift;
    private final int segmentMask;

    /**
     * Constructs a new HashMap with the specified ZooKeeper client and serializer, located at the specifed base znode
     *<p>
     * Note: {@code mapNode} <em>must </em> exist in zookeeper before calling this constructor, or else an exception
     * will be thrown when first attempting to use this map.
     *
     * <p>This constructor builds a map which places all elements with
     * the {@link org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE} privileges in ZooKeeper.
     *
     * @param mapNode the znode to use
     * @param zkSessionManager the session manager for the ZooKeeper instance
     * @param serializer the serializer to use
     */
    public ZkHashMap(String mapNode, ZkSessionManager zkSessionManager,Serializer<Entry<K,V>> serializer){
        this(mapNode,zkSessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE,serializer);
    }

    /**
     * Constructs a new HashMap with the specified ZooKeeper client and serializer,
     * located at the specifed base znode, and using the specified concurrency level
     *<p>
     * Note: {@code mapNode} <em>must </em> exist in zookeeper before calling this constructor, or else an exception
     * will be thrown when first attempting to use this map.
     *
     * <p>This constructor builds a map which places all elements with
     * the {@link org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE} privileges in ZooKeeper.
     *
     * @param mapNode the znode to use
     * @param zkSessionManager the session manager for the ZooKeeper instance
     * @param serializer the serializer to use
     * @param concurrency the estimated number of concurrently updating parties.
     */
    public ZkHashMap(String mapNode, ZkSessionManager zkSessionManager,Serializer<Entry<K,V>> serializer, int concurrency){
        this(mapNode,zkSessionManager, ZooDefs.Ids.OPEN_ACL_UNSAFE,serializer,concurrency);
    }

    /**
     * Constructs a new HashMap with the specified ZooKeeper client and serializer,
     * located at the specifed base znode, with the default concurrency level and the specified ZooKeeper privileges.
     *<p>
     * Note: {@code mapNode} <em>must </em> exist in zookeeper before calling this constructor, or else an exception
     * will be thrown when first attempting to use this map.
     *
     * @param mapNode the znode to use
     * @param zkSessionManager the session manager for the ZooKeeper instance
     * @param privileges the ZooKeeper privileges to use
     * @param serializer the serializer to use
     */
    public ZkHashMap(String mapNode, ZkSessionManager zkSessionManager,
                     List<ACL> privileges, Serializer<Entry<K,V>> serializer) {
        this(mapNode, zkSessionManager,privileges,serializer,16);
    }

    /**
     * Constructs a new HashMap with the specified ZooKeeper client and serializer,
     * located at the specifed base znode, with the specified concurrency level and ZooKeeper privileges.
     *<p>
     * Note: {@code mapNode} <em>must </em> exist in zookeeper before calling this constructor, or else an exception
     * will be thrown when first attempting to use this map.
     *
     * @param mapNode the znode to use
     * @param zkSessionManager the session manager for the ZooKeeper instance
     * @param privileges the ZooKeeper privileges to use
     * @param serializer the serializer to use
     * @param concurrency the estimated number of concurrently updating parties.
     */
    public ZkHashMap(String mapNode, ZkSessionManager zkSessionManager,
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
        Lock mapLock = new ReentrantZkLock(mapNode,zkSessionManager,privileges);
        mapLock.lock();
        try{
            //need to read the data out of zookeeper
            ZooKeeper zk = zkSessionManager.getZooKeeper();
            for(int i=0;i< segments.length;i++){
                try {
                    //attach the first segments to any segments which are already created
                    List<String> zkBuckets = ZkUtils.filterByPrefix(zk.getChildren(mapNode,false),"bucket");
                    ZkUtils.sortBySequence(zkBuckets,'-');
                    int numBucketsBuilt = 0;
                    for(int bucketIndex=0;bucketIndex<zkBuckets.size()&&bucketIndex< segments.length;bucketIndex++){
                        numBucketsBuilt++;
                        segments[bucketIndex] = new ZkSegment<K,V>(mapNode+"/"+zkBuckets.get(bucketIndex),
                                serializer,zkSessionManager,privileges);
                    }

                    //create any additional segments as needed
                    while(numBucketsBuilt< segments.length){
                        String bucketNode = ZkUtils.safeCreate(zk, mapNode + "/bucket-",
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
        for(ZkSegment bucket: segments){
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
        for(ZkSegment bucket: segments){
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
        for(ZkSegment bucket: segments){
            bucket.acquireReadLock();
        }
        try{
            for(ZkSegment bucket: segments){
                if(bucket.containsValue(value))
                    return true;
            }
            return false;
        }finally{
            for(ZkSegment bucket: segments){
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
        for (ZkSegment bucket : segments)
            bucket.clear();
    }

    @Override
    public Set<K> keySet() {
        return new KeySet();
    }

    @Override
    public Collection<V> values() {
        return new ValuesCollection();
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The set is backed by the map, so changes to the map are
     * reflected in the set, and vice-versa.  The set supports element
     * removal, which removes the corresponding mapping from the map,
     * via the <tt>Iterator.remove</tt>, <tt>Set.remove</tt>,
     * <tt>removeAll</tt>, <tt>retainAll</tt>, and <tt>clear</tt>
     * operations.  It does not support the <tt>add</tt> or
     * <tt>addAll</tt> operations.
     *
     * <p>The view's <tt>iterator</tt> is a "weakly consistent" iterator
     * that will never throw {@link ConcurrentModificationException},
     * and guarantees to traverse elements as they existed upon
     * construction of the iterator, and may (but is not guaranteed to)
     * reflect any modifications subsequent to construction.
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return new EntrySet();
    }

/*-------------------------------------------------------------------------------------------------------------------*/
    /*private helper methods and classes*/

    /*
     * Applies a supplemental hash function to a given hashCode, which
     * defends against poor quality hash functions.  This is critical
     * because ZkHashMap uses power-of-two length hash tables,
     * that otherwise encounter collisions for hashCodes that do not
     * differ in lower or upper bits.
     *
     * -sf- stolen shamelessly from the ConcurrentHashMap.
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

    /*Finds the bucket which holds the data for the given hashCode*/
    private ZkSegment<K,V> bucketFor(int hash) {
        return segments[(hash >>> segmentShift) & segmentMask];
    }

    /*
     * Represents a chunk of the data stored in the Map. Each element which
     * is inserted into the containing map is hashed to a particular segment, which then manages
     * how to store that element in a specialized znode under the base mapNode.
     *
     */
    private static final class ZkSegment<K,V>{
        private static final String entryPrefix = "entry";
        private static final char entryDelimiter = '-';
        private final String bucketNode;
        private final ZkSessionManager sessionManager;
        private final List<ACL> nodePrivileges;
        private final ReadWriteLock safety;
        private final Serializer<Entry<K,V>> serializer;

        private ZkSegment(String bucketNode, Serializer<Entry<K,V>> serializer,
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
                        Map.Entry<K,V> deserializedEntry = serializer.deserialize(data);
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
                    byte[] data = ZkUtils.safeGetData(zk, bucketNode + "/" + match, false, new Stat());
                    if(data.length>0){
                        Map.Entry<K,V> deserializedEntry = serializer.deserialize(data);
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
                        Map.Entry<K,V> deserializedEntry = serializer.deserialize(data);
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

        boolean replace(K key,int hash, V oldValue, V newValue){
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
                        Map.Entry<K,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)&&deserializedEntry.getValue().equals(oldValue)){
                            //we can set the value
                            zk.setData(bucketNode+"/"+match,serializer.serialize(new AbstractMap.SimpleEntry<K,V>(key,newValue)),-1);
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

        V replace(K key, int hash, V newValue){
            Lock writeLock = safety.writeLock();
            writeLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false), getHashedPrefix(hash));
                for(String match: hashMatches){
                    byte[] data = ZkUtils.safeGetData(zk,bucketNode+"/"+match,false,new Stat());
                    if(data.length>0){
                        Map.Entry<K,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)){
                            //we can set the value
                            zk.setData(bucketNode+"/"+match,serializer.serialize(new AbstractMap.SimpleEntry<K,V>(key,newValue)),-1);
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

        V put(K key, int hash, V value,boolean onlyIfAbsent){
            Lock writeLock = safety.writeLock();
            writeLock.lock();
            try{
                ZooKeeper zk = sessionManager.getZooKeeper();
                List<String> hashMatches = ZkUtils.filterByPrefix(zk.getChildren(bucketNode, false), entryPrefix + "-" + hash);
                for(String match: hashMatches){

                    byte[] data = ZkUtils.safeGetData(zk,bucketNode+"/"+match,false, new Stat());
                    if(data.length>0){
                        Map.Entry<K,V> deserializedEntry = serializer.deserialize(data);
                        if(deserializedEntry.getKey().equals(key)){
                            if(onlyIfAbsent){
                                //it isn't absent, so we can't put it
                                return deserializedEntry.getValue();
                            }
                            //we found our match, serialize and set the data
                            zk.setData(bucketNode+"/"+match,serializer.serialize(new AbstractMap.SimpleEntry<K,V>(key,value)),-1);
                            return deserializedEntry.getValue();
                        }
                    }
                }

                //if we make it this far, we didn't find any matches, so just create another entry in this bucket
                zk.create(bucketNode+"/"+ entryPrefix + entryDelimiter +hash+ entryDelimiter,serializer.serialize(new AbstractMap.SimpleEntry<K,V>(key,value)),nodePrivileges, CreateMode.PERSISTENT_SEQUENTIAL);
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
                        Map.Entry<K,V> deserializedEntry = serializer.deserialize(data);
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

        private String getHashedPrefix(int hash) {
            return entryPrefix + entryDelimiter + hash;
        }

        public Iterator<Entry<K, V>> entryIterator() {
            return new ZkReadWriteIterator<Entry<K,V>>(bucketNode,serializer,sessionManager,nodePrivileges,entryPrefix,entryDelimiter,safety);
        }

        @SuppressWarnings({"unchecked"})
        private static <K,V> ZkSegment<K,V>[] newArray(int size) {
            return new ZkSegment[size];
        }
    }
    

    /*Iterator support*/

    private Iterator<Map.Entry<K,V>> entryIterator() {
        List<Iterator<Entry<K,V>>> iterators = new ArrayList<Iterator<Entry<K,V>>>(segments.length);
        for(ZkSegment<K,V> segment:segments){
            iterators.add(segment.entryIterator());
        }
        return new CompositeIterator<Entry<K,V>>(iterators);
    }

    private class EntrySet extends AbstractSet<Entry<K,V>>{
        @Override
        public Iterator<Entry<K, V>> iterator() {return entryIterator();}

        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> e = (Map.Entry<?,?>)o;
            V v = ZkHashMap.this.get(e.getKey());
            return v != null && v.equals(e.getValue());
        }

        public boolean remove(Object o) {
            if (!(o instanceof Map.Entry))
                return false;
            Map.Entry<?,?> e = (Map.Entry<?,?>)o;
            return ZkHashMap.this.remove(e.getKey(), e.getValue());
        }

        public int size() {return ZkHashMap.this.size();}
        public void clear() {ZkHashMap.this.clear();}
    }

    private class KeySet extends AbstractSet<K>{

        @Override
        public Iterator<K> iterator() {return new KeyIterator();}

        @Override
        public int size() {return ZkHashMap.this.size();}

        @Override
        public boolean contains(Object o) {return ZkHashMap.this.containsKey(o);}

        @Override
        public boolean remove(Object o) {return ZkHashMap.this.remove(o)!=null;}
    }

    private class ValuesCollection extends AbstractCollection<V>{

        @Override
        public Iterator<V> iterator() {return new ValueIterator();}

        @Override
        public int size() {return ZkHashMap.this.size();}

        public boolean contains(Object o) {return ZkHashMap.this.containsValue(o);}
        public void clear() {ZkHashMap.this.clear();}
    }

    private abstract class BaseIterator{
        private final Iterator<Map.Entry<K,V>> delegate;

        public BaseIterator() {this.delegate = entryIterator();}

        public boolean hasMoreElements() {return hasNext();}

        public Map.Entry<K,V> nextEntry() {return delegate.next();}

        public boolean hasNext() {return delegate.hasNext();}

        public void remove() {delegate.remove();}
    }

    private class KeyIterator extends BaseIterator implements Iterator<K>, Enumeration<K>{

        @Override
        public K nextElement() {return next();}

        @Override
        public K next() {return super.nextEntry().getKey();}
    }

    private class ValueIterator extends BaseIterator implements Iterator<V>, Enumeration<V>{

        @Override
        public V nextElement() {return next();}

        @Override
        public V next() {return super.nextEntry().getValue();}
    }


}
