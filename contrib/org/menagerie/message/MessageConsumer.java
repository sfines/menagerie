package org.menagerie.message;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.ZkPrimitive;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Consumer for a Message Queue
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 03-Jan-2011
 *          Time: 19:24:08
 */
public class MessageConsumer {
    public static final String messagePrefix = "message";
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final SyncListener listener;

    public MessageConsumer(String baseNode, ZkSessionManager zkSessionManager, List<ACL>privileges) {
        listener = new SyncListener(baseNode,zkSessionManager,privileges);
    }

    public interface Listener{
        public void messageReceived(byte[] message);
    }

    public synchronized void startListener(){
        if(listener.running)
            return; //already running, don't bother starting
        executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    listener.listen();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    public void stopListener(){
        listener.stopListening();
    }

    public void addMessageListener(Listener listener) {
        this.listener.addMessageListener(listener);
    }

    public void removeMessageListener(Listener listener) {
        this.listener.removeMessageListener(listener);
    }

    private static class SyncListener extends ZkPrimitive {
        private volatile boolean running = false;
        private final List<Listener> messageListeners = new CopyOnWriteArrayList<Listener>();
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        /**
         * Creates a new SyncListener with the correct node information.
         *
         * @param baseNode         the base node to use
         * @param zkSessionManager the session manager to use
         * @param privileges       the privileges for this node.
         */
        protected SyncListener(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
            super(baseNode, zkSessionManager, privileges);
        }

        public void listen() throws InterruptedException, KeeperException {
            running=true;
            final String myNodeName = zkSessionManager.getZooKeeper().create(baseNode + "/listener-", emptyNode, privileges, CreateMode.EPHEMERAL_SEQUENTIAL);
            while(running){
                localLock.lock();
                try{
                    final ZooKeeper zk = zkSessionManager.getZooKeeper();
                    final List<String> messages = ZkUtils.filterByPrefix(zk.getChildren(baseNode,signalWatcher),messagePrefix);
                    //submit the messages to be processed
                    executor.submit(new Runnable() {
                        @Override
                        public void run() {
                            processMessages(messages, zk, myNodeName);
                        }
                    });
                    //now wait for the next message to be received
                    condition.await();
                }finally{
                    localLock.unlock();
                }
            }
        }

        public void stopListening(){
            running = false;
            notifyAll();
        }

        public void addMessageListener(Listener listener){
            messageListeners.add(listener);
        }

        public void removeMessageListener(Listener listener){
            messageListeners.remove(listener);
        }

        private void processMessages(List<String> messages, ZooKeeper zk, String myNodeName) {
            for(String message:messages){
                /*
                We don't need any local locking here, because we are clear that we are using
                a SingleThreadExecutor, so that these runnables are guaranteed to run in order of
                arrival, on a single thread, making this whole runnable need no synchronization.
                 */
                //process each message in turn
                final byte[] data;
                try {
                    if(zk.exists(baseNode+"/"+message+"/listener-"+ ZkUtils.parseSequenceNumber(myNodeName,'-'),false)!=null){
                        //we've already processed this message, so don't do anything
                        continue;
                    }
                    data = zk.getData(baseNode+"/"+message,false,new Stat());
                    //send the message out to all the listeners
                    informListeners(data);
                    //tell the rest of the cluster that you've processed this message, so that it can be removed
                    //by a cleaner thread, if necessary
                    zk.create(baseNode+"/"+message+"/listener-"+ZkUtils.parseSequenceNumber(myNodeName,'-'),emptyNode,privileges, CreateMode.PERSISTENT);
                } catch (KeeperException e) {
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void informListeners(byte[] data) {
            for(Listener listener:messageListeners){
                listener.messageReceived(data);
            }
        }
    }
}
