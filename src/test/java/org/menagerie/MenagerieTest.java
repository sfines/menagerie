/*
 * Copyright 2010 Scott Fines
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.menagerie;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * Base class for Menagerie tests, to provide convenience setup and tearDown methods.
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 20-Jan-2011
 *          Time: 09:27:02
 */
public abstract class MenagerieTest {
    protected static final String hostString = "localhost:2181";
    protected static final String testPath = "/test";
    protected static final int timeout = 2000;
    protected static ZkSessionManager zkSessionManager;

    protected static ZooKeeper zk;

    @Before
    public void setup() throws Exception {
        zk = newZooKeeper();

        //be sure that the lock-place is created
        zk.create(testPath,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zkSessionManager = new BaseZkSessionManager(zk);

        prepare();
    }

    /**
     * Prepare for the next test by constructing new objects, etc, as necessary
     */
    protected abstract void prepare();

    @After
    public void tearDown() throws Exception{
        close();
        try{
           ZkUtils.recursiveSafeDelete(zk, testPath,-1);

        }catch(KeeperException ke){
            //suppress because who cares what went wrong after our tests did their thing?
        }finally{
            zk.close();
        }
    }

    /**
     * Cleanup the subclass after a test has ran
     */
    protected abstract void close();


    /**
     * Creates a new test ZooKeeper instance
     *
     * @return a new ZooKeeper instance
     * @throws IOException if there is trouble connecting to ZooKeeper
     */
    protected static ZooKeeper newZooKeeper() throws IOException {
        return new ZooKeeper(hostString, timeout,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
//                System.out.println(event);
            }
        });
    }
}
