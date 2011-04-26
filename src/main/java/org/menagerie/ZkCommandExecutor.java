package org.menagerie;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: Apr 25, 2011
 *         Time: 1:55:52 PM
 */
public class ZkCommandExecutor {
    private static final Logger logger = Logger.getLogger(ZkCommandExecutor.class);
    private static final long DEFAULT_DELAY_MILLIS = 1000;
    public static final int DEFAULT_MAX_RETRIES = 3;

    private final int maxRetries;
    private final long delayMillis;
    private final ZkSessionManager sessionManager;


    public ZkCommandExecutor(ZkSessionManager sessionManager) {
        this(sessionManager,DEFAULT_MAX_RETRIES,DEFAULT_DELAY_MILLIS,TimeUnit.MILLISECONDS);
    }

    public ZkCommandExecutor(ZkSessionManager sessionManager,long retryDelay,TimeUnit retryUnits) {
        this(sessionManager,DEFAULT_MAX_RETRIES,retryDelay,retryUnits);
    }

    public ZkCommandExecutor(ZkSessionManager sessionManager,int maxRetries) {
        this(sessionManager,maxRetries,DEFAULT_DELAY_MILLIS,TimeUnit.MILLISECONDS);
    }

    public ZkCommandExecutor(ZkSessionManager sessionManager, int maxRetries,long delayMillis, TimeUnit retryUnit){
        this.sessionManager = sessionManager;
        this.maxRetries = maxRetries;
        this.delayMillis = retryUnit.toMillis(delayMillis);
    }


    public <T> T execute(ZkCommand<T> command) throws KeeperException {
        KeeperException exception = null;
        for(int retry=0;retry<maxRetries;retry++){
            try{
                return command.execute(sessionManager.getZooKeeper());
            } catch (InterruptedException e) {
                logger.debug("Command"+command+" was interrupted. Retrying");
                doWait(retry, delayMillis);
            } catch (KeeperException.ConnectionLossException kce) {
                if(exception==null){
                    exception = kce;
                }
                logger.debug("Connection was lost while comamnd "+ command +"was attempted. Retrying");
                doWait(retry, delayMillis);
            } catch(KeeperException.SessionExpiredException kse){
                if(exception==null){
                    exception = kse;
                }
                logger.info("Session expired while comamnd "+ command +"was attempted. Retrying with new session");
                throw kse;
            }
        }
        if(exception==null){
            //we got repeated interruptions, so throw a KeeperException of the correct type
            throw new RuntimeException("Repeatedly interrupted during an uninterruptible task caused Task to fail");
        }
        throw exception;

    }

    public <T> T executeInterruptibly(ZkCommand<T> command) throws InterruptedException, KeeperException {
        KeeperException exception = null;
        for(int retry=0;retry<maxRetries;retry++){
            try{
                return command.execute(sessionManager.getZooKeeper());
            } catch (KeeperException.ConnectionLossException kce) {
                if(exception==null){
                    exception = kce;
                }
                logger.debug("Connection was lost while comamnd "+ command +"was attempted. Retrying");
                doWaitInterruptibly(retry, delayMillis);
            } catch(KeeperException.SessionExpiredException kse){
                if(exception==null){
                    exception = kse;
                }
                logger.debug("Session expired while comamnd "+ command +"was attempted. Retrying with new session");
                doWaitInterruptibly(retry, delayMillis);
            }
        }
        throw exception;
    }

    private void doWait(int retry, long retryDelay) {
        try {
            Thread.sleep(retry*retryDelay);
        } catch (InterruptedException e) {
            logger.debug("sleep interrupted");
        }
    }

    private void doWaitInterruptibly(int retry, long retryDelay) throws InterruptedException{
        Thread.sleep(retry*retryDelay);
    }
    


}
