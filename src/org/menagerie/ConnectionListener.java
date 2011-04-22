/*
 * Copyright 2010 Scott Fines
 *
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

/**
 * A Listener which is fired when certain connection events happen with respect to the
 * ZooKeeper service.
 * <p>
 *
 * @author Scott Fines
 * @version 1.0
 */
public interface ConnectionListener {

    /**
     * Fired to indicated that the ZooKeeper client has reconnected to the ZooKeeper service after having
     * been disconnected, but before the session timeout has expired.
     */
    public void syncConnected();

    /**
     * Fired to indicate that the ZooKeeper Session has expired.
     * <p>
     * When this has been called, some data structures (any structure based on ephemeral nodes) may no longer be
     * in a valid state.
     *
     * <p>NOTE: ZooKeeper does NOT fire session expired events until AFTER a reconnect to ZooKeeper occurs.
     * In the event of a total network partition which lasts forever, no expiration event will be fired. This
     * Means that this method may never be called. If understanding that this behavior is necessary, polling with
     * timeout is required. To do this, use {@link org.menagerie.ZkSessionPoller} manually, or as an underlying part
     * of your {@link org.menagerie.ZkSessionManager} instance. {@link org.menagerie.DefaultZkSessionManager} has
     * an implementation which will poll for these session events.
     */
    public void expired();

    /**
     * Indicates that the connection to ZooKeeper has been lost.
     *
     * <p>Connections to ZooKeeper may be lost due to network partitions or due to ZooKeeper servers failing. In
     * many situations, becoming disconnected is transient, as the client may quickly reconnect to another server
     * in the ensemble. If, however, the client cannot connect to a ZooKeeper server within the specified session
     * timeout, then the ZooKeeper session is terminated by the ensemble, and the client transitions to the expired
     * state. Note, however, that the client is <em>not</em> guaranteed to receive notification of session expiration,
     * only of disconnection. It is therefore recommended that services which are subject to disconnection generally
     * cease operations, or at the very least pause operations, while a connection is re-established.
     *
     */
    public void disconnected();
}
