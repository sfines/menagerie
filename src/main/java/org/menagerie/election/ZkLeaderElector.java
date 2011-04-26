/*
 * Copyright 2010 Scott Fines
 *
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
package org.menagerie.election;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.menagerie.ZkSessionManager;
import org.menagerie.ZkUtils;
import org.menagerie.locks.ReentrantZkLock;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * ZooKeeper-based implementation of a synchronous Leader-election protocol.
 * <p>
 *
 * @author Scott Fines
 * @version 1.0
 */
public final class ZkLeaderElector implements LeaderElector{
    private static final String electionPrefix = "election";
    private static final char electionDelimiter ='-';
    private final String name;
    private final ElectionZkLock lock;

    /**
     * Constructs a new LeaderElector to enact Leader-Elections synchronously.
     *
     * @param baseNode the node to use for elections
     * @param zkSessionManager the session manager to use
     * @param privileges the privileges for this Elector.
     */
    public ZkLeaderElector(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
        this.lock = new ElectionZkLock(baseNode,zkSessionManager,privileges);
        try{
            name = InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean nominateSelfForLeader() {
        //attempt to get the lock
        return lock.tryLock();
    }

    @Override
    public boolean nominateSelfForLeader(long timeout, TimeUnit unit) throws InterruptedException{
        return lock.tryLock(timeout,unit);
    }

    @Override
    public void concede() {
        lock.unlock();
    }

    /**
     * Returns the IP address of the leader node, as a String.
     *
     * @inheritDoc
     * @return the IP address of the leader node, as a String.
     */
    @Override
    public String getLeader() {
        try {
            List<String> nominatedElements = ZkUtils.filterByPrefix(lock.getZooKeeper().getChildren(lock.getBaseNode(), false), electionPrefix);
            ZkUtils.sortBySequence(nominatedElements,electionDelimiter);

            String leader = nominatedElements.get(0);
            int startIndex = leader.indexOf(electionPrefix) + 1;
            return leader.substring(startIndex,leader.indexOf(electionDelimiter,startIndex));
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class ElectionZkLock extends ReentrantZkLock {

        protected ElectionZkLock(String baseNode, ZkSessionManager zkSessionManager, List<ACL> privileges) {
            super(baseNode, zkSessionManager, privileges);
        }

        @Override
        protected String getBaseLockPath() {
            return baseNode + "/" + electionPrefix+electionDelimiter+name + electionDelimiter;
        }

        @Override
        protected String getLockPrefix() {
            return electionPrefix;
        }

        public ZooKeeper getZooKeeper(){
            return zkSessionManager.getZooKeeper();
        }

        public String getBaseNode(){
            return baseNode;
        }
    }
}
