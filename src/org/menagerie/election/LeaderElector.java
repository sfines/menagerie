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
     */
    public boolean nominateSelfForLeader();

    /**
     * Attempts to become the leader, waiting up to a maximum of {@code timeout} units.
     *
     * @param timeout the maximum time to wait to become leader
     * @param unit the time units to use
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
