package org.menagerie.locks;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkPrimitive;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 16-Dec-2010
 *          Time: 20:13:19
 */
public class ZkCondition extends ZkPrimitive implements Condition {
    private static final String conditionPrefix="condition";
    private static final char conditionDelimiter = '-';
    private final ReentrantZkLock distributedLock;

    public ZkCondition(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges, ReentrantZkLock lock) {
        super(baseNode, zkSessionManager, privileges);
        this.distributedLock = lock;
    }

    @Override
    public void await() throws InterruptedException {
        await(Long.MAX_VALUE,TimeUnit.DAYS);
    }



    @Override
    public void awaitUninterruptibly() {

    }

    @Override
    public long awaitNanos(long nanosTimeout) throws InterruptedException {
        return awaitNanos(nanosTimeout);
    }

    private long awaitNanos(long nanosTimeout,boolean interruptable) throws InterruptedException {
        if(!distributedLock.hasLock())
           throw new IllegalMonitorStateException("await() was called without owning the associated lock");

        //put a signal node onto zooKeeper, then wait for it to be deleted
        try {
            ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
            String conditionName = zooKeeper.create(baseNode + "/" + conditionPrefix + conditionDelimiter, emptyNode, privileges, CreateMode.EPHEMERAL_SEQUENTIAL);
            //now release the associated zkLock
            distributedLock.unlock();
            long timeLeft  = nanosTimeout;
            while(true){
                long start = System.nanoTime();
                boolean acquired = false;
                try{
                    acquired = localLock.tryLock(timeLeft, TimeUnit.NANOSECONDS);
                }catch(InterruptedException ie){
                    //suppress if not interruptable
                    if(interruptable){
                        zooKeeper.delete(conditionName,-1);
                        throw ie;
                    }
                }
                try{
                    long end = System.nanoTime();
                    timeLeft -=(end-start);
                    if(timeLeft<=0||!acquired){
                        //delete the condition node myself
                        zooKeeper.delete(conditionName,-1);
                        return timeLeft;
                    }else{
                        //we still have some time left over
                        if(zooKeeper.exists(conditionName,this)==null){
                            //we have been signalled, so relock and then return
                            start = System.nanoTime();
                            boolean reacquired = false;
                            try{
                                reacquired = distributedLock.tryLock(timeLeft, TimeUnit.NANOSECONDS);
                            }catch(InterruptedException ie){
                                //suppress if not interruptable
                                if(interruptable){
                                    zooKeeper.delete(conditionName,-1);
                                    throw ie;
                                }
                            }
                            end = System.nanoTime();
                            timeLeft -=(end-start);
                            if(!reacquired||timeLeft<=0){
                                //timed out
                                zooKeeper.delete(conditionName,-1);
                            }
                            return timeLeft;
                        }else{
                            timeLeft = condition.awaitNanos(timeLeft);
                        }
                    }
                }finally{
                    localLock.unlock();
                }
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        return awaitNanos(unit.toNanos(time))>0;
    }

    @Override
    public boolean awaitUntil(Date deadline) throws InterruptedException {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void signal() {
        if(!distributedLock.hasLock())
            throw new IllegalMonitorStateException("Signal is attempted without first owning the signalling lock");
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        try {
            List<String> conditionsToSignal = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode, false), conditionPrefix);

            //delete the lowest numbered waiting party
            ZkUtils.sortBySequence(conditionsToSignal,conditionDelimiter);
            zooKeeper.delete(baseNode+"/"+conditionsToSignal.get(0),-1);

        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void signalAll() {
        if(!distributedLock.hasLock())
            throw new IllegalMonitorStateException("Signal is attempted without first owning the signalling lock");
        ZooKeeper zooKeeper = zkSessionManager.getZooKeeper();
        try {
            List<String> conditionsToSignal = ZkUtils.filterByPrefix(zooKeeper.getChildren(baseNode, false), conditionPrefix);

            //notify all waiting conditions in sequence
            ZkUtils.sortBySequence(conditionsToSignal,conditionDelimiter);

            for(String condition:conditionsToSignal){
                zooKeeper.delete(baseNode+"/"+condition,-1);
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
