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

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
    private static final Logger logger = Logger.getLogger(DefaultZkSessionManager.class);
    //this could potentially be a very write-heavy list, so a synchronized list will perform better
    //than a more traditional CopyOnWriteArrayList would be
    private List<ConnectionListener> listeners = Collections.synchronizedList(new ArrayList<ConnectionListener>());

    private ZooKeeper zk;
    private final String connectionString;
    private final int timeout;
    private final ExecutorService executor;
    private volatile boolean shutdown;
    private ZkSessionPoller poller;
    private final int zkSessionPollInterval;

    /**
     * Creates a new instance of a DefaultZkSessionManager.
     *
     * <p>This is equivalent to calling {@code new DefaultZkSessionManager(connectionString, timeout, -1)}
     *
     * @param connectionString the string to connect to ZooKeeper with (in the form of (serverIP):(port),...)
     * @param timeout the timeout to use before expiring the ZooKeeper session.
     */
    public DefaultZkSessionManager(String connectionString, int timeout) {
        this(connectionString,timeout,Executors.newSingleThreadExecutor(),-1);
    }

    /**
     * Creates a new instance of a DefaultZkSessionManager, using the specified zkSessionPollInterval to
     * determine the polling interval to use for determining session expiration events.
     *
     * <p>If {@code zkSessionPollInterval} is less than zero, then no polling will be executed.
     *
     * @param connectionString the string to connect to ZooKeeper with (in the form of (serverIP):(port),...)
     * @param timeout the timeout to use before ZooKeeper expires a session
     * @param zkSessionPollInterval the polling interval to use for manual session checking, or -1 if no
     *          manual session checking is to be used
     */
    public DefaultZkSessionManager(String connectionString, int timeout, int zkSessionPollInterval){
        this(connectionString,timeout,Executors.newSingleThreadExecutor(),zkSessionPollInterval);
    }

    /**
     * Creates a new instance of a DefaultZkSessionManager, using the specified ExecutorService to handle
     * ConnectionListener api calls.
     *
     *
     * <p>This is equivalent to calling {@code new DefaultZkSessionManager(connectionString, timeout,executor, -1)}
     *
     * @param connectionString the string to connect to ZooKeeper with (in the form of (serverIP):(port),...)
     * @param timeout the timeout to use before expiring the ZooKeeper session.
     * @param executor the executor to use in constructing calling threads.
     */
    public DefaultZkSessionManager(String connectionString, int timeout,ExecutorService executor) {
        this(connectionString,timeout,executor,-1);
    }

    /**
     * Creates a new instance of a DefaultZkSessionManager, using the specified zkSessionPollInterval to
     * determine the polling interval to use for determining session expiration events, and using the
     * specified ExecutorService to handle ConnectionListener api calls.
     *
     * <p>If {@code zkSessionPollInterval} is less than zero, then no polling will be executed.
     *
     * @param connectionString the string to connect to ZooKeeper with (in the form of (serverIP):(port),...)
     * @param timeout the timeout to use before ZooKeeper expires a session
     * @param executor the executor to use in constructing calling threads
     * @param zkSessionPollInterval the polling interval to use for manual session checking, or -1 if no
     *          manual session checking is to be used
     */
    public DefaultZkSessionManager(String connectionString, int timeout, ExecutorService executor, int zkSessionPollInterval){
        this.connectionString = connectionString;
        this.timeout = timeout;
        this.executor = executor;
        this.zkSessionPollInterval = zkSessionPollInterval;
    }


    @Override
    public synchronized ZooKeeper getZooKeeper() {
        if(shutdown)
            throw new IllegalStateException("Cannot request a ZooKeeper after the session has been closed!");
        if(zk==null || zk.getState()==ZooKeeper.States.CLOSED){
            try {
                zk = new ZooKeeper(connectionString,timeout,new SessionWatcher(this));
            } catch (IOException e) {
                logger.error(e);
                throw new RuntimeException(e);
            }
            if(zkSessionPollInterval>0){
                //stop any previous polling, if it hasn't been stopped already
                if(poller!=null){
                    poller.stopPolling();
                }
                //create a new poller for this ZooKeeper instance
                poller = new ZkSessionPoller(zk,zkSessionPollInterval,new SessionPollListener(zk,this));
                poller.startPolling();
            }
        }else{
            //make sure that your zookeeper instance is synced
            final CountDownLatch latch = new CountDownLatch(1);
            zk.sync("/",new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx) {
                    latch.countDown();
                }
            }, this);
            try {
                latch.await();
            } catch (InterruptedException e) {
                //shouldn't be interruptible, so just continue on, resetting the interrupt status
                Thread.currentThread().interrupt();
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
        notifyState(event.getState());
    }

    private void notifyState(final Watcher.Event.KeeperState state) {
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

    private static class SessionPollListener implements ConnectionListener{
        private final ZooKeeper zk;
        private final DefaultZkSessionManager sessionManager;

        private SessionPollListener(ZooKeeper zk, DefaultZkSessionManager sessionManager) {
            this.zk = zk;
            this.sessionManager = sessionManager;
        }

        @Override
        public void syncConnected() {
            //do nothing, since we aren't interested
        }

        @Override
        public void expired() {
            logger.info("Session expiration has been detected. Notifying all connection listeners and cleaning up ZooKeeper State");
            //notify applications
            sessionManager.notifyState(Watcher.Event.KeeperState.Expired);
            //shut down this ZooKeeper instance
            try {
                zk.close();
            } catch (InterruptedException e) {
                logger.warn("An InterruptedException was detected while attempting to close a ZooKeeper instance; ignoring because we're shutting it down anyway");
            }
        }
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
