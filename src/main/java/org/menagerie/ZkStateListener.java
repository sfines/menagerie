package org.menagerie;

/**
 * @author Scott Fines
 *         Date: Apr 22, 2011
 *         Time: 9:09:14 AM
 */
public interface ZkStateListener {

    public void syncConnected();

    public void sessionExpired();

    public void disconnected();
    
}
