package org.menagerie.locks;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.ConnectionListener;
import org.menagerie.ZkPrimitive;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * A ZooKeeper-based implementation of a fair, Reentrant Mutex Lock.
 * <p>
 * A {@code ReentrantZkLock} is <i>owned</i> by the <i>party</i> (both the current thread and the running ZooKeeper client)
 *  which has last successfully locked it, but has not yet unlocked it. Parties invoking {@code lock} will return,
 * successfully acquiring the lock, whenever the lock is not owned by another party. All methods will return
 * immediate success if the current party is the lock owner. This can be checked by {@link #hasLock()}.
 * <p>
 * Note that this implementation is fully <i>fair</i>; multiple concurrent requests by different parties will be granted
 * <i>in order</i>, where the ordering is determined by the ZooKeeper server.
 * <p>
 * This lock supports {@code Integer.MAX_VALUE} repeated locks by the same thread. Attempts to exceed this result
 * in overflows which can result in an improper lock state.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 13:01:59
 */
public class ReentrantZkLock extends ZkPrimitive implements Lock, ConnectionListener {
    private static final String lockPrefix = "lock";
    protected static final char lockDelimiter = '-';

    private ThreadLocal<LockHolder> locks = new ThreadLocal<LockHolder>();

    protected ReentrantZkLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        super(baseNode, zkSessionManager, privileges);
        zkSessionManager.addConnectionListener(this);
    }

    @Override
    public final void lock() {
        try {
            tryLock(-1,TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            //this will only be thrown by ZooKeeper-based utilities, by construction, so throw it as a runtime exception
            throw new RuntimeException(e);
        }
    }

    @Override
    public final void lockInterruptibly() throws InterruptedException {
        //TODO -sf- should this do something different?
        tryLock(Long.MAX_VALUE,TimeUnit.DAYS);
    }

    @Override
    public final boolean tryLock() {
        LockHolder local = locks.get();
        if(local!=null){
            local.incrementLock();
            return true;
        }
        ZooKeeper zk = zkSessionManager.getZooKeeper();
        try {
            String lockNode = zk.create(getBaseLockPath(),emptyNode,privileges, CreateMode.EPHEMERAL_SEQUENTIAL);

            //try to determine its position in the queue
            boolean lockAcquired = askForLock(zk, lockNode,false);
            if(!lockAcquired){
                //we didn't get the lock, so return false
                zk.delete(lockNode,-1);
                return false;
            }else{
                //we have the lock, so it goes on the queue
                locks.set(new LockHolder(lockNode));
                return true;
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public final boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        LockHolder local = locks.get();
        if(local!=null){
            local.incrementLock();
            return true;
        }
        ZooKeeper zk = zkSessionManager.getZooKeeper();

        String lockNode;
        try {
            lockNode = zk.create(getBaseLockPath(),emptyNode,privileges, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch(InterruptedException ie){
            throw new RuntimeException(ie);
        }
        while(true){
            boolean localAcquired = localLock.tryLock(time, unit);
            try{
                if(!localAcquired){
                    //delete the lock node and return false
                    zk.delete(lockNode,-1);
                    return false;
                }
                //ask ZooKeeper for the lock
                boolean acquiredLock = askForLock(zk, lockNode,true);

                if(!acquiredLock){
                    //we don't have the lock, so we need to wait for our watcher to fire
                    if(time>0){
                        //specified a timeout, so let's use it
                        boolean alerted = condition.await(time, unit);
                        if(!alerted){
                            //we timed out, so delete the node and return false
                            //delete the lock node and return false
                            zk.delete(lockNode,-1);
                            return false;
                        }
                    }else{
                        //no timeout specified, so wait forever if necessary
                        try{
                            condition.await();
                        }catch(InterruptedException ie){
                            return false;
                        }
                    }
                }else{
                    //we have the lock, so return happy
                    locks.set(new LockHolder(lockNode));
                    return true;
                }
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            } finally{
                localLock.unlock();
            }
        }
    }

    /**
     * @return true if the current party(i.e. Thread and ZooKeeper client) owns the Lock
     */
    public final boolean hasLock(){
        return locks.get()!=null;
    }

    @Override
    public final void unlock() {
        LockHolder nodeToRemove = locks.get();
        if(nodeToRemove==null)
            throw new IllegalMonitorStateException("Attempting to unlock without first obtaining that lock on this thread");

        int numLocks = nodeToRemove.decrementLock();
        if(numLocks==0){
            locks.remove();
            try {
                zkSessionManager.getZooKeeper().delete(nodeToRemove.lockNode(),-1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException("Conditions are not yet supported by ZooKeeper locks");
    }

    @Override
    public void syncConnected() {
        localLock.lock();
        try{
            condition.signalAll();
        }finally{
            localLock.unlock();
        }
    }

    @Override
    public synchronized void expired() {
        //TODO -sf- this isn't right, gotta do something else
        //our session expired, so all of our locks are gone, we'll need to re-acquire all of them
        locks = new ThreadLocal<LockHolder>();
        localLock.lock();
        try{
            condition.signalAll();
        }finally{
            localLock.unlock();
        }
    }

    /**
     * Asks ZooKeeper for a lock of a given type.
     * <p>
     * Classes which override this method MUST adhere to the requested watch rule, or else the semantics
     * of the lock interface may be broken.
     * <p>
     * It is recommended that classes which override this method also override {@link #getBaseLockPath()} as well.
     *
     * @param zk the ZooKeeper client to use
     * @param lockNode the node to lock on
     * @param watch whether or not to watch other nodes if the lock is behind someone else
     * @return true if the lock has been acquired, false otherwise
     * @throws KeeperException if Something bad happens on the ZooKeeper server
     * @throws InterruptedException if communication between ZooKeeper and the client fail in some way
     */
    protected boolean askForLock(ZooKeeper zk, String lockNode, boolean watch) throws KeeperException, InterruptedException {
        List<String> locks = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),getLockPrefix());
        ZkUtils.sortBySequence(locks,lockDelimiter);

        String myNodeName = lockNode.substring(lockNode.lastIndexOf('/')+1);
        int myPos = locks.indexOf(myNodeName);

        int nextNodePos = myPos-1;
        while(nextNodePos>=0){
            Stat stat;
            if(watch)
                stat=zk.exists(baseNode+"/"+locks.get(nextNodePos),this);
            else
                stat=zk.exists(baseNode+"/"+locks.get(nextNodePos),false);
            if(stat!=null){
                //there is a node which already has the lock, so we need to wait for notification that that
                //node is gone
                return false;
            }else{
                nextNodePos--;
            }
        }
        return true;
    }

    /**
     * @return the prefix to prepend to all nodes created by this lock.
     */
    protected String getLockPrefix() {
        return lockPrefix;
    }

    /**
     * Gets the base path for a lock node of this type.
     *
     * @return the base lock path(all the way up to a delimiter for sequence elements)
     */
    protected String getBaseLockPath(){
        return baseNode+"/"+getLockPrefix()+lockDelimiter;
    }

/*-------------------------------------------------------------------------------------------------------------------*/
    /*private helper classes and methods*/

    /*Holder for information about a specific lock*/
    private static class LockHolder{
        private final String lockNode;
        private final AtomicInteger numLocks = new AtomicInteger(1);

        private LockHolder(String lockNode) {
            this.lockNode = lockNode;
        }

        public void incrementLock(){
            numLocks.incrementAndGet();
        }

        public int decrementLock(){
            return numLocks.decrementAndGet();
        }

        public String lockNode() {
            return lockNode;
        }
    }
}
