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
 * <p>
 * This implementation guarantees that all calls to {@code ConnectionListener}s will occur
 * on a separate, dedicated thread which is provided by a ThreadExecutorService. The default constructions
 * will create an ExecutorService for this, but the caller may specify a specific ExecutorService upon
 * construction.
 *
 * @author Scott Fines
 * @version 1.0
 */
public class DefaultZkSessionManager implements ZkSessionManager{
    private List<ConnectionListener> listeners = new CopyOnWriteArrayList<ConnectionListener>();

    private ZooKeeper zk;
    private final String connectionString;
    private final int timeout;
    private final ExecutorService executor;
    private volatile boolean shutdown;

    /**
     * Creates a new instance of a DefaultZkSessionManager.
     *
     * @param connectionString the string to connect to ZooKeeper with (in the form of <serverIP>:<port>,...)
     * @param timeout the timeout to use before expiring the ZooKeeper session.
     */
    public DefaultZkSessionManager(String connectionString, int timeout) {
        this(connectionString,timeout,Executors.newSingleThreadExecutor());
    }

    /**
     * Creates a new instance of a DefaultZkSessionManager, using the specified ExecutorService to handle
     * ConnectionListener api calls.
     *
     * @param connectionString the string to connect to ZooKeeper with (in the form of <serverIP>:<port>,...)
     * @param timeout the timeout to use before expiring the ZooKeeper session.
     * @param executor the executor to use in constructing calling threads.
     */
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
                zk = new ZooKeeper(connectionString,timeout,new SessionWatcher(this));
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

/*--------------------------------------------------------------------------------------------------------------------*/
    /*private helper methods */

    private void notifyListeners(WatchedEvent event) {
        final Watcher.Event.KeeperState state = event.getState();
        executor.submit(new Runnable() {
            @Override
            public void run() {
                if(state== Watcher.Event.KeeperState.Expired){
                    //tell everyone that all their watchers and ephemeral nodes have been removed--suck
                    for(ConnectionListener listener:listeners){
                        listener.expired();
                    }
                    zk = null;
                }else if(state== Watcher.Event.KeeperState.SyncConnected){
                    //tell everyone that we've reconnected to the Server, and they should make sure that their watchers
                    //are in place
                    for(ConnectionListener listener:listeners){
                        listener.syncConnected();
                    }
                }
            }
        });
    }

    private static class SessionWatcher implements Watcher{
        private final DefaultZkSessionManager manager;

        private SessionWatcher(DefaultZkSessionManager manager) {
            this.manager = manager;
        }

        @Override
        public void process(WatchedEvent event) {
            manager.notifyListeners(event);
        }
    }

}
