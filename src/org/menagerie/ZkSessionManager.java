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

import org.apache.zookeeper.ZooKeeper;

/**
 * Session Manager for a single ZooKeeper client.
 * <p>
 * This is in place to abstract away the details of managing a ZooKeeper client, and to allow
 * multiple different objects to attach themselves as connection listeners on the same ZooKeeper
 * object.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 20-Nov-2010
 *          Time: 17:11:32
 */
public interface ZkSessionManager {

    /**
     * Gets the ZooKeeper client object to use.
     * <p>
     * This method should always return a ZooKeeper object that is neither expired nor closed. In the case
     * of connection failure (due to network partitions, etc), then this object is not necessarily in a valid state,
     * but it should never be in an expired state.
     * <p>
     * If this session manager object has been closed via a call to {@link #closeSession()}, then an
     * IllegalStateException will be thrown.
     *
     * @return a ZooKeeper instance to use
     * @throws IllegalStateException if this SessionManager has been closed
     * @throws RuntimeException if a connection problem occurs with the ZooKeeper service
     */
    ZooKeeper getZooKeeper();

    /**
     * Closes this Session, and renders all future calls to {@link #getZooKeeper()} invalid.
     */
    void closeSession();

    /**
     * Add a connection listener to listen for connection events.
     *
     * @param listener the connection listener to add
     */
    void addConnectionListener(ConnectionListener listener);

    /**
     * Remove the specified connection listener from the session manager.
     *
     * @param listener the listener to be removed.
     */
    void removeConnectionListener(ConnectionListener listener);
}
