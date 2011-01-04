package org.menagerie.message;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkSessionManager;

import java.util.List;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 03-Jan-2011
 *          Time: 19:53:27
 */
public class MessageProducer {
    private final ZkSessionManager sessionManager;
    private final List<ACL> privileges;
    private final String baseNode;

    public MessageProducer(ZkSessionManager sessionManager, List<ACL> privileges, String baseNode) {
        this.sessionManager = sessionManager;
        this.privileges = privileges;
        this.baseNode = baseNode;
    }

    public void publishMessage(byte[] messageData){
        try {
            sessionManager.getZooKeeper().create(baseNode+"/message-",messageData,privileges, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
}
