package org.menagerie.latches;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.locks.ZkLocks;

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;

/**
 * ZooKeeper-based implementation of a CyclicBarrier.
 * <p>
 * Wherever possible, this implementation is consistent with the semantics of
 * {@link java.util.concurrent.CyclicBarrier}, and adheres to as many of the same constraints as are reasonable
 * to expect.
 * <p>
 * A CyclicBarrier causes <i>all</i> threads which call the {@code await()} methods to wait for the barrier to be completed
 * across all participating nodes. The cyclic nature allows the same instance to be repeatedly used, by calling the
 * {@link #reset()} methods.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 09-Dec-2010
 *          Time: 19:56:17
 */
public class ZkCyclicBarrier extends AbstractZkBarrier {
    private static final char delimiter ='-';
    private static final String barrierPrefix="party";
    private boolean reset;

    /**
     * Creates a new CyclicBarrier, or joins a CyclicBarrier which has been previously created by another node/thread
     * on the same latchNode.
     * <p>
     * This constructor checks to ensure that the barrier is in a good state before returning. If the latchNode was the
     * site of a previously broken barrier, then the Barrier is reset as if the {@link #reset()} method was called.
     * <p>
     * When this constructor returns, the latch is guaranteed to be in a clear, unbroken state and is ready to be
     * used.
     *
     * @param zkSessionManager the ZkSessionManager to use
     * @param barrierNode the node the execute the barrier under
     * @param privileges the privileges for this latch
     * @param size the number of elements which must enter the barrier before threads may proceed.
     */
    ZkCyclicBarrier(long size, ZkSessionManager zkSessionManager, String barrierNode, List<ACL> privileges) {
        super(size,barrierNode, zkSessionManager,privileges);

        ensureNodeExists();
        try {
            checkReset();
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Waits until all parties have been invoked on this barrier.
     * <p>
     * If the current thread is not the last to arrive, then it is disabled for thread-scheduling purposes until one
     * of the following conditions are realized:
     * <p>
     *  1.  The Barrier is reached by another thread/node
     *  2.  Some other thread interrupts the current thread
     *  3.  The Barrier has been broken by another party
     *  4.  The Barrier has been reset by another party
     *  5.  Some other party times out waiting for the barrier to complete
     * <p>
     * If the Barrier is broken upon entry into this method, or another party breaks the Barrier at any time while
     * this thread is waiting, or another party times out while this thread is waiting,
     *  a {@link java.util.concurrent.BrokenBarrierException} will be thrown.
     * <p>
     * If the Barrier is reset while this thread is waiting, then a {@link java.util.concurrent.BrokenBarrierException}
     * will be thrown.
     * <p>
     * If the waiting thread is interrupted for reasons unrelated to ZooKeeper, then the state of the Barrier will
     * be set to Broken, all other waiting parties will throw a BrokenBarrierException, and an InterruptedException
     * will be thrown on this thread.
     *
     * @return the index of this thread with respect to all other parties in the Barrier. If this thread is the
     *          first to arrive, index is {@link #getParties()}-1; the last thread to arrive will return 0
     * @throws InterruptedException if the local thread is interrupted, or if there is a communication error
     *          with ZooKeeper
     * @throws BrokenBarrierException if another party breaks the barrier, or another party calls reset while
     *          waiting for the Barrier to complete
     */
    public long await() throws InterruptedException, BrokenBarrierException {
        try {
            return await(Long.MAX_VALUE,TimeUnit.DAYS);
        } catch (TimeoutException e) {
            //should never happen, since we're waiting for forever. Still, throw it anyway, just in case
            throw new RuntimeException(e);
        }
    }

    /**
     * Waits until all parties have been invoked on this barrier.
     * <p>
     * If the current thread is not the last to arrive, then it is disabled for thread-scheduling purposes until one
     * of the following conditions are realized:
     * <p>
     *  1.  The Barrier is reached by another thread/node
     *  2.  Some other thread interrupts the current thread
     *  3.  The Barrier has been broken by another party
     *  4.  The Barrier has been reset by another party
     *  5.  Some other party times out waiting for the barrier to complete
     *  6.  This thread times out waiting for the barrier to complete
     * <p>
     * If the Barrier is broken upon entry into this method, or another party breaks the Barrier at any time while
     * this thread is waiting, or another party times out while this thread is waiting,
     *  a {@link java.util.concurrent.BrokenBarrierException} will be thrown.
     * <p>
     * If the Barrier is reset while this thread is waiting, then a {@link java.util.concurrent.BrokenBarrierException}
     * will be thrown.
     * <p>
     * If the waiting thread is interrupted for reasons unrelated to ZooKeeper, then the state of the Barrier will
     * be set to Broken, all other waiting parties will throw a BrokenBarrierException, and an InterruptedException
     * will be thrown on this thread.
     * <p>
     * If this thread times out waiting for the barrier to complete, the barrier state will be set to broken (Causing a
     * BrokenBarrierException to be thrown on all other parties), and a TimeoutException will be thrown.
     *
     * @param timeout the maximum time to wait
     * @param unit the TimeUnit to use for timeouts.
     * @return the index of this thread with respect to all other parties in the Barrier. If this thread is the
     *          first to arrive, index is {@link #getParties()}-1; the last thread to arrive will return 0
     * @throws InterruptedException if the local thread is interrupted, or if there is a communication error
     *          with ZooKeeper
     * @throws BrokenBarrierException if another party breaks the barrier, or another party calls reset while
     *          waiting for the Barrier to complete
     * @throws TimeoutException if this thread times out waiting for the barrier to complete.
     */
    public long await(long timeout, TimeUnit unit) throws InterruptedException, BrokenBarrierException, TimeoutException {
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        String myNode;
        try {
            //ensure that the state of the barrier isn't broken
            if(isBroken()){
                //barrier has been broken
                throw new BrokenBarrierException(new String(zooKeeper.getData(getBrokenPath(),false,new Stat())));
            }
            /*
            This must be Persistent_sequential instead of ephemeral_sequential because, if this thread creates this
            element, then dies before the barrier is entered again, then the next count will be one smaller than it
            should be, potentially causing the Barrier to never reach the total needed, resulting in a distributed
            deadlock.
            */
            myNode = zooKeeper.create(getBarrierBasePath(), emptyNode, privileges, CreateMode.PERSISTENT_SEQUENTIAL);
        } catch (KeeperException e) {
            throw new InterruptedException(e.getMessage());
        }
        zkSessionManager.addConnectionListener(this);
        while(true){
            localLock.lock();
            try {
                if(isBroken()){
                    //barrier has been broken
                    zkSessionManager.removeConnectionListener(this);
                    throw new BrokenBarrierException(new String(zooKeeper.getData(getBrokenPath(),false,new Stat())));
                }else if(isReset(zooKeeper)){
                    zkSessionManager.removeConnectionListener(this);
                    throw new BrokenBarrierException("The Barrier has been reset");
                }
                List<String> children = ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,this),barrierPrefix);
                long count = children.size();
                if(count>=total){
                    zkSessionManager.removeConnectionListener(this);
                    //latch has been used, so remove latchReady key
                    if(zooKeeper.exists(getReadyPath(),false)!=null){
                        ZkUtils.safeDelete(zooKeeper,getReadyPath(),-1);
                    }
                    //find this node
                    ZkUtils.sortBySequence(children,delimiter);
                    return children.indexOf(myNode.substring(myNode.lastIndexOf("/")+1));
                }else{
                    try{
                        boolean alerted = condition.await(timeout,unit);
                        System.out.println("Thread-id="+Thread.currentThread().getName()+", Alerted="+alerted);
                        if(!alerted){
                            zkSessionManager.removeConnectionListener(this);
                            breakBarrier("Timeout occurred waiting for barrier to complete");
                            throw new TimeoutException("Timed out waiting for barrier to complete");
                        }
                    }catch(InterruptedException ie){
                        //we've been interrupted locally, so we need to let everyone know
                        zkSessionManager.removeConnectionListener(this);
                        breakBarrier(ie.getMessage());
                        throw ie;
                    }
                }
            } catch (KeeperException e) {
                zkSessionManager.removeConnectionListener(this);
                throw new InterruptedException(e.getMessage());
            } finally{
                localLock.unlock();
            }
        }
    }

    /**
     * @return the number of parties currently waiting for the barrier to complete.
     */
    public long getNumberWaiting(){
        try {
            return highestChild( ZkUtils.filterByPrefix(zkSessionManager.getZooKeeper().getChildren(baseNode,false),barrierPrefix),delimiter);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @return the number of parties required to trip this barrier
     */
    public long getParties(){
        return total;
    }

    /**
     * @return true if this barrier is broken.
     */
    public boolean isBroken(){
        try {
            return zkSessionManager.getZooKeeper().exists(getBrokenPath(),false)!=null;
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Resets this barrier.
     * <p>
     * If any parties are waiting for this barrier when this method is called, a BrokenBarrierException will be thrown.
     * <p>
     * Note: It is recommended that only one party call this method at a time, to ensure consistency across the cluster.
     * This may be accomplished using a Leader-election protocol. It may be preferable to simply create a new
     * Barrier instance on all nodes.
     */
    public void reset(){
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        //TODO -sf- is there a non-blocking way to do this?
        Lock lock = ZkLocks.newReentrantLock(zkSessionManager, baseNode, privileges);

        try {
            lock.lock();
            doReset(zooKeeper);

        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            lock.unlock();
        }
    }


    /*------------------------------------------------------------------------------------------------------------------*/
    /*private helper methods*/

    /*
     * Checks the state of the Barrier, and calls reset if necessary
     */
    private void checkReset() throws KeeperException, InterruptedException {
        //determine if we need to reset the state of the barrier
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        //TODO -sf- is there a non-blocking way to do this?
        Lock lock = ZkLocks.newReentrantLock(zkSessionManager, baseNode, privileges);

        try {
            lock.lock();
            if(isBroken()||isReset(zooKeeper)||zooKeeper.exists(getReadyPath(),false)==null){
                //need to reset
                doReset(zooKeeper);
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            lock.unlock();
        }
    }

    private void doReset(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        //put a reset node on the path to let people know it's been reset
        if(zooKeeper.exists(getResetPath(),false)==null){
            try{
                zooKeeper.create(getResetPath(),emptyNode,privileges, CreateMode.EPHEMERAL);
            }catch(KeeperException ke){
                if(ke.code()!=KeeperException.Code.NODEEXISTS)
                    throw ke;
            }
        }
        //remove the latchReady state
        if(zooKeeper.exists(getReadyPath(),false)!=null){
            ZkUtils.safeDelete(zooKeeper,getReadyPath(),-1);
        }

        clearState(zooKeeper,barrierPrefix,delimiter);
        //remove any broken state setters
        if(isBroken()){
            zooKeeper.delete(getBrokenPath(),-1);
        }
        //remove the reset node, since we're back in a good state
        zooKeeper.delete(getResetPath(),-1);

        //put the good state node in place
        zooKeeper.create(getReadyPath(),emptyNode,privileges,CreateMode.PERSISTENT);
    }

    /*
     * Breaks the barrier for all parties.
     */
    private void breakBarrier(String message) {
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName+": Breaking barrier");
        if(!isBroken()){
            System.out.println(threadName+": Barrier not already broken, breaking it now");
            try {
                zkSessionManager.getZooKeeper().create(getBrokenPath(),message.getBytes(),privileges,CreateMode.PERSISTENT);
                System.out.println(threadName+": Barrier broken");
            } catch (KeeperException e) {
                //if the node already exists, someone else broke it first, so don't worry about it
                if(e.code()!=KeeperException.Code.NODEEXISTS)
                    throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


    private String getBrokenPath() {
        return baseNode+"/"+"cyclicBarrier"+delimiter+BarrierState.BROKEN;
    }

    private String getResetPath(){
        return baseNode+"/"+"cyclicBarrier"+delimiter+BarrierState.RESET;
    }

    private String getReadyPath(){
        return baseNode+"/"+"cyclicBarrier"+delimiter+BarrierState.LATCH_READY;
    }


    private String getBarrierBasePath(){
        return baseNode+"/"+barrierPrefix+delimiter;
    }

    private boolean isReset(ZooKeeper zk) throws InterruptedException, KeeperException {
        return zk.exists(getResetPath(),false)!=null;
    }

    private enum BarrierState{
        LATCH_READY,
        BROKEN,
        RESET
    }

}
