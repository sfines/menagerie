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
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 08-Jan-2011
 *          Time: 12:42:37
 */
@Beta
public class ZkIterator<T> extends ZkPrimitive implements Iterator<T> {
    private volatile List<String> currentList;
    private volatile int cursor = 0;
    private volatile String currentNode;
    private volatile byte[] currentData;
    private volatile boolean removeCalled = false;

    private final Serializer<T> serializer;
    private final IteratorWatcher updater;
    private volatile boolean initialized = false;
    private final Executor executor = Executors.newSingleThreadExecutor();
    private final char iteratorDelimiter;


    public ZkIterator(String baseNode, Serializer<T> serializer, ZkSessionManager zkSessionManager, List<ACL> privileges,String iteratorPrefix,char iteratorDelimiter) {
        super(baseNode,zkSessionManager,privileges);
        this.serializer = serializer;
        this.iteratorDelimiter = iteratorDelimiter;

        updater = new IteratorWatcher(baseNode,zkSessionManager,privileges,iteratorPrefix,iteratorDelimiter);
    }

    public synchronized void initIterator(){
        if(initialized)
            throw new IllegalMonitorStateException("Cannot initialize the same iterator twice!");
        executor.execute(new Runnable() {
            @Override
            public void run() {
                updater.updateIterator();
            }
        });
        initialized = true;
    }

    public synchronized void closeIterator(){
        updater.stop();
    }

    @Override
    public boolean hasNext() {
        ZooKeeper zk = zkSessionManager.getZooKeeper();
        for(String child:currentList){
            int pos = ZkUtils.parseSequenceNumber(child,iteratorDelimiter);
            if(pos>cursor){
                try {
                    currentData = ZkUtils.safeGetData(zk,baseNode+"/"+child,false, new Stat());
                    if(currentData.length>0){
                        currentNode = child;
                        return true;
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        //stop trying to update this iterator, it's been used up
        closeIterator();
        currentData = null;
        return false;
    }

    @Override
    public T next() {
        if(currentData==null) throw new NoSuchElementException();
        cursor = ZkUtils.parseSequenceNumber(currentNode,iteratorDelimiter);
        removeCalled = false;
        return serializer.deserialize(currentData);
    }

    @Override
    public void remove() {
        if(removeCalled)
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

    private class IteratorWatcher extends ZkPrimitive{
        private volatile boolean running;
        private final String prefix;
        private final char prefixDelimiter;

        /**
         * Creates a new ZkPrimitive with the correct node information.
         *
         * @param baseNode         the base node to use
         * @param zkSessionManager the session manager to use
         * @param privileges       the privileges for this node.
         */
        protected IteratorWatcher(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges,String iteratorPrefix, char prefixDelimiter) {
            super(baseNode, zkSessionManager, privileges);
            this.prefix = iteratorPrefix;
            this.prefixDelimiter = prefixDelimiter;
        }

        public void updateIterator(){
            while(running){
                localLock.lock();
                try{
                    List<String> children = ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode, signalWatcher), prefix);
                    ZkUtils.sortBySequence(children,prefixDelimiter);

                    currentList = children;

                    condition.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } finally{
                    localLock.unlock();
                }
            }
        }

        public synchronized void stop(){
            running=false;
            this.notifyParties();
        }

        public boolean isRunning(){
            return running;
        }
    }
}
