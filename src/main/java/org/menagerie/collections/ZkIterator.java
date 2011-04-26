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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.*;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A ZooKeeper-based general-purpose iterator.
 * <p>
 * This class provides the mechanisms needed to iterate over all the children of a specified znode which matches
 * a pre-determined predicate.
 *
 * <p>Like many direct iterator implementations, this is <em>not</em> thread-safe, and is intended to be used
 * by only one thread. This implementation also does not provide any ZooKeeper-based synchronization protocols. Classes
 * which need to provide any ZooKeeper synchronization should extend this class to provide the necessary synchronization.
 *
 * <p>This iterator is <i>weakly-consistent</i>, and will never
 * through a {@link java.util.ConcurrentModificationException}.
 *  
 * @author Scott Fines
 * @version 1.0
 * @param <T> the type of elements to be retrieved from the iterator
 *          Date: 08-Jan-2011
 *          Time: 12:42:37
 */
class ZkIterator<T> extends ZkPrimitive implements Iterator<T> {
    private int cursor = 0;
    private String currentNode;
    private byte[] currentData;
    private boolean removeCalled = false;

    private final Serializer<T> serializer;
    private final char iteratorDelimiter;
    private final String prefix;

    /**
     * Constructs a new iterator.
     *
     * @param baseNode the znode to iterate over
     * @param serializer the serializer to use to deserialize the entries
     * @param zkSessionManager the ZooKeeper Session Manager to use
     * @param privileges the privileges that this znode requires
     * @param iteratorPrefix the prefix of interest
     * @param iteratorDelimiter the delimiter separating the prefix from sequence numbers.
     */
    public ZkIterator(String baseNode, Serializer<T> serializer, ZkSessionManager zkSessionManager,
                      List<ACL> privileges,String iteratorPrefix,char iteratorDelimiter) {
        super(baseNode,zkSessionManager,privileges);
        this.serializer = serializer;
        this.iteratorDelimiter = iteratorDelimiter;
        this.prefix = iteratorPrefix;

    }

    @Override
    public boolean hasNext() {
        ZooKeeper zk = zkSessionManager.getZooKeeper();
        try {
            //get the most up-to-date-list
            List<String> children = ZkUtils.filterByPrefix(zk.getChildren(baseNode, false),prefix);
            //now go through it until you find a position higher than your current cursor
            ZkUtils.sortBySequence(children,iteratorDelimiter);
            for(String child:children){
                int pos = ZkUtils.parseSequenceNumber(child,iteratorDelimiter);

                if(pos>=cursor){
                    currentData = ZkUtils.safeGetData(zk,baseNode+"/"+child,false, new Stat());
                    if(currentData.length>0){
                        currentNode = child;
                        return true;
                    }
                }
            }
            currentData=null;
            return false;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T next() {
        if(currentData==null) throw new NoSuchElementException();
        cursor = ZkUtils.parseSequenceNumber(currentNode,iteratorDelimiter)+1;
        removeCalled = false;
        return serializer.deserialize(currentData);
    }

    @Override
    public void remove() {
        if(removeCalled||currentNode==null)
            throw new IllegalStateException("Remove called without first calling next!");
        try {
            ZkUtils.safeDelete(zkSessionManager.getZooKeeper(),baseNode+"/"+currentNode,-1);
            currentNode = null;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        removeCalled = true;
    }

    @Override
    public String toString() {
        return "ZkIterator{ baseNode = " + baseNode+"}" ;
    }
}
