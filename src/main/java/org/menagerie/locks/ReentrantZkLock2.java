package org.menagerie.locks;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Will eventually replace the ReentrantZkLock when I'm sure it's good enough.
 * @author Scott Fines
 *         Date: Apr 25, 2011
 *         Time: 4:12:06 PM
 */
@Beta
public class ReentrantZkLock2 implements Lock {
    private static final byte[] emptyBytes = new byte[]{};
    private final ZkCommandExecutor executor;
    private final Lock localLock;
    private final String machineId;
    private final List<ACL> privileges;
    private final String baseNode;
    private final Condition condition;
    private final SignalWatcher signalWatcher;

    private final LockHolder holder = new LockHolder();


    public ReentrantZkLock2(String baseNode, ZkSessionManager zkSessionManager){
        this(baseNode,new ZkCommandExecutor(zkSessionManager),ZooDefs.Ids.OPEN_ACL_UNSAFE);
    }

    public ReentrantZkLock2(String baseNode, ZkSessionManager zkSessionManager,List<ACL> privileges){
        this(baseNode,new ZkCommandExecutor(zkSessionManager),privileges);
    }

    public ReentrantZkLock2(String baseNode,ZkCommandExecutor executor, List<ACL> privileges) {
        this.executor = executor;
        this.privileges = privileges;
        this.baseNode = baseNode;
        localLock = new ReentrantLock(true); 
        condition = localLock.newCondition();
        try{
            machineId = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        signalWatcher = new SignalWatcher();

        ensureNodeExists();
    }

    @Override
    public void lock() {
        if(holder.increment())return; //we have successful reentrancy
        try{
            final String lockNode = createLockNode();
            executor.execute(new ZkCommand<Void>() {
                @Override
                public Void execute(ZooKeeper zk) throws KeeperException, InterruptedException {
                    while(true){
                        localLock.lock();
                        try{
                            boolean acquired = tryAcquireDistributed(zk,lockNode,true);
                            if(!acquired){
                                condition.awaitUninterruptibly();
                            }else{
                                //we have the lock, so we're done here
                                holder.setHoldingThread(lockNode);
                                return null;
                            }
                        }finally{
                            localLock.unlock();
                        }
                    }
                }
            });
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        tryLock(Long.MAX_VALUE,TimeUnit.DAYS);
    }

    @Override
    public boolean tryLock() {
        try {
            final String lockNode = createLockNode();
            Boolean acquired = executor.execute(new ZkCommand<Boolean>() {
                @Override
                public Boolean execute(ZooKeeper zk) throws KeeperException, InterruptedException {
                    return tryAcquireDistributed(zk, lockNode, false);
                }
            });
            if(!acquired){
                doCleanup(lockNode);
            }
            return acquired;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
        if(Thread.interrupted())
            throw new InterruptedException();
        if(holder.increment()) return true; //already have the lock
        try {
            long timeLeftNanos = timeUnit.toNanos(l);
            long start = System.nanoTime();
            final String lockNode = createLockNodeInterruptibly();
            long end = System.nanoTime();
            timeLeftNanos = timeLeftNanos-(end-start);

            if(Thread.interrupted()){
                doCleanup(lockNode);
                throw new InterruptedException();
            }
            if(timeLeftNanos<0){
                doCleanup(lockNode);
                return false;
            }

            boolean acquired = executor.executeInterruptibly(new TimedZkCommand<Boolean>(timeLeftNanos) {
                @Override
                public Boolean execute(ZooKeeper zk) throws KeeperException, InterruptedException {
                    long timeoutNanos = timeLeft;

                    while(timeoutNanos>0){
                        if(Thread.interrupted()){
                            doCleanup(lockNode);
                            throw new InterruptedException();
                        }
                        localLock.lockInterruptibly();
                        try{
                            long start = System.nanoTime();
                            boolean acquired = tryAcquireDistributed(zk,lockNode,true);
                            long end = System.nanoTime();
                            timeoutNanos -=(end-start);
                            if(!acquired){
                                timeoutNanos = condition.awaitNanos(timeoutNanos);
                            }else{
                                holder.setHoldingThread(lockNode);
                                return true;
                            }
                        }finally{
                            localLock.unlock();
                        }
                    }
                    return false;
                }
            });
            if(!acquired){
                doCleanup(lockNode);
            }
            return acquired;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void unlock() {
        int remaining = holder.decrement();
        if(remaining==0){
            try {
                executor.execute(new ZkCommand<Void>() {
                    @Override
                    public Void execute(ZooKeeper zk) throws KeeperException, InterruptedException {
                        ZkUtils.safeDelete(zk,holder.getLockNode(),-1);
                        return null;
                    }
                });
            } catch (KeeperException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Condition newCondition() {
        throw new UnsupportedOperationException();
    }

    protected String getLockPrefix() {
        return "lock";
    }

    private String getBaseLockPath(){
        return baseNode+"/"+getPartyId();
    }

    protected boolean tryAcquireDistributed(ZooKeeper zk, String lockNode, boolean watch) throws KeeperException, InterruptedException {
        List<String> locks = ZkUtils.filterByPrefix(zk.getChildren(baseNode,false),getLockPrefix());
        ZkUtils.sortBySequence(locks,'-');
        String myNodeName = lockNode.substring(lockNode.lastIndexOf('/')+1);
        int myPos = locks.indexOf(myNodeName);

        int nextNodePos = myPos-1;
        while(nextNodePos>=0){
            Stat stat;
            if(watch)
                stat=zk.exists(baseNode+"/"+locks.get(nextNodePos),signalWatcher);
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

/*--------------------------------------------------------------------------------------------------------------------*/
    /*private helper methods*/

    private void doCleanup(final String lockNode) throws KeeperException {
        executor.execute(new ZkCommand<Void>() {
            @Override
            public Void execute(ZooKeeper zk) throws KeeperException, InterruptedException {
                ZkUtils.safeDelete(zk,lockNode,-1);
                return null;
            }
        });
    }

    private String getPartyId(){
        return getLockPrefix()+'-'+machineId+'-'+Thread.currentThread().getId()+'-';
    }

    private String createLockNode() throws KeeperException {
        return executor.execute(getCreateCommand());
    }

    private String createLockNodeInterruptibly() throws KeeperException,InterruptedException{
        return executor.executeInterruptibly(getCreateCommand());
    }

    private ZkCommand<String> getCreateCommand() {
        return new ZkCommand<String>() {
            @Override
            public String execute(ZooKeeper zk) throws KeeperException, InterruptedException {
                String node = getNodeCommand().execute(zk);
                if(node!=null)
                    return node;

                //nobody exists with my party's unique name, so create one and return it
                return ZkUtils.safeCreate(zk, getBaseLockPath(), emptyBytes, privileges, CreateMode.EPHEMERAL_SEQUENTIAL);
            }
        };
    }

    private ZkCommand<String> getNodeCommand(){
        return new ZkCommand<String>() {
            @Override
            public String execute(ZooKeeper zk) throws KeeperException, InterruptedException {
                List<String> lockChildren = ZkUtils.filterByPrefix(zk.getChildren(baseNode, false), getLockPrefix());
                //see if lockChildren contains an entry with my unique identifier. If it does, return that
                String myLockId = getPartyId();
                for (String lockChild : lockChildren) {
                    if (lockChild.startsWith(myLockId)) {
                        return lockChild;
                    }
                }
                return null;
            }
        };
    }

    private void ensureNodeExists() {
        try {
            executor.execute(new ZkCommand<Void>() {
                @Override
                public Void execute(ZooKeeper zk) throws KeeperException, InterruptedException {
                    ZkUtils.recursiveSafeCreate(zk,baseNode,emptyBytes,privileges, CreateMode.PERSISTENT);
                    return null;
                }
            });
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    private class SignalWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            localLock.lock();
            try{
                condition.signalAll();
            }finally{
                localLock.unlock();
            }
        }
    }

    private static abstract class TimedZkCommand<T> implements ZkCommand<T>{
        protected final long timeLeft;

        protected TimedZkCommand(long timeLeft) {
            this.timeLeft = timeLeft;
        }
    }

    private class LockHolder{
        private final AtomicReference<Thread> holdingThread = new AtomicReference<Thread>();
        private volatile String lockNode;
        private final AtomicInteger holdCount = new AtomicInteger(0);

        public void setHoldingThread(String lockNode){
            holdingThread.set(Thread.currentThread());
            holdCount.set(1);
            this.lockNode = lockNode;
        }

        public boolean increment(){
            if(Thread.currentThread().equals(holdingThread.get())){
                holdCount.incrementAndGet();
                return true;
            }else{
                return false;
            }
        }
        public int decrement(){
            if(Thread.currentThread().equals(holdingThread.get())){
                int count = holdCount.decrementAndGet();
                if(count<=0){
                    holdingThread.set(null);
                }
                return count;
            }else{
                return holdCount.get();
            }
        }

        public String getLockNode(){
            return lockNode;
        }
    }
}

