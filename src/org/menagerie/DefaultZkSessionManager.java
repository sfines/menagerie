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
package org.menagerie;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * A Default implementation of a {@link ZkSessionManager}.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 08:41:36
 */
public class DefaultZkSessionManager implements ZkSessionManager, Watcher {
    private List<ConnectionListener> listeners = new CopyOnWriteArrayList<ConnectionListener>();

    private ZooKeeper zk;
    private final String connectionString;
    private final int timeout;
    private final ExecutorService executor;
    private volatile boolean shutdown;

    public DefaultZkSessionManager(String connectionString, int timeout) {
        this.connectionString = connectionString;
        this.timeout = timeout;
        this.executor = Executors.newSingleThreadExecutor();
    }

    public DefaultZkSessionManager(String connectionString, int timeout,ExecutorService executor) {
        this.connectionString = connectionString;
        this.timeout = timeout;
        this.executor = executor;
    }


    @Override
    public synchronized ZooKeeper getZooKeeper() {
        if(shutdown)
            throw new IllegalStateException("Cannot request a ZooKeeper after the session has been closed!");
        if(zk==null || zk.getState()==ZooKeeper.States.CLOSED){
            try {
                zk = new ZooKeeper(connectionString,timeout,this);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return zk;
    }

    @Override
    public synchronized void closeSession() {
        try{
            if(zk!=null){
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }finally{
            executor.shutdown();
            shutdown=true;
        }
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeConnectionListener(ConnectionListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void process(WatchedEvent event) {
        final Event.KeeperState state = event.getState();
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if(state==Event.KeeperState.Expired){
                    //tell everyone that all their watchers and ephemeral nodes have been removed--suck
                    for(ConnectionListener listener:listeners){
                        listener.expired();
                    }
                    zk = null;
                }else if(state==Event.KeeperState.SyncConnected){
                    //tell everyone that we've reconnected to the Server, and they should make sure that their watchers
                    //are in place
                    for(ConnectionListener listener:listeners){
                        listener.syncConnected();
                    }
                }
            }
        });
    }
}
