package org.menagerie;

/**
 * A Listener which is fired when certain connection events happen with respect to the
 * ZooKeeper service.
 * <p>
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 08:40:55
 */
public interface ConnectionListener {

    /**
     * Fired to indicated that the ZooKeeperService has become connected after a disconnect event
     */
    public void syncConnected();

    /**
     * Fired to indicate that the ZooKeeper Service has expired and it is necessary to re-establish watchers
     * and ephemeral nodes.
     */
    public void expired();
}
