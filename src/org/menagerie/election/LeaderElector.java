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
package org.menagerie.election;

import java.util.concurrent.TimeUnit;

/**
 * Interface for synchronously managing Leader Elections across distributed networks.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 16:35:56
 */
public interface LeaderElector {

    /**
     * Attempts to become the leader, returning immediately with either failure or success
     * @return true if this thread is the leader
     */
    public boolean nominateSelfForLeader();

    /**
     * Attempts to become the leader, waiting on this thread up to a maximum of {@code timeout} units.
     *
     * @param timeout the maximum time to wait to become leader
     * @param unit the time units to use
     * @return true if this thread is the leader
     * @throws InterruptedException if the thread is interrupted while waiting to determine the leadership
     */
    public boolean nominateSelfForLeader(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * Concedes the election to someone else.
     * <p>
     * This MUST be called on the same thread as {@code nominateSelfForLeader} was called.
     */
    public void concede();

    /**
     * @return the IP address of the leader node
     */
    public String getLeaderIp();

}
