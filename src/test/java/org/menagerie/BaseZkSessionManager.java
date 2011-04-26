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
package org.menagerie;

import org.apache.zookeeper.ZooKeeper;

/**
 * TODO -sf- document!
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 08:54:27
 */
public class BaseZkSessionManager implements ZkSessionManager {
    private final ZooKeeper zk;

    public BaseZkSessionManager(ZooKeeper zk) {
        this.zk = zk;
    }

    @Override
    public ZooKeeper getZooKeeper() {
        return zk;
    }

    @Override
    public void closeSession() {
        try {
            zk.close();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        //default no-op
    }

    @Override
    public void removeConnectionListener(ConnectionListener listener) {
        //default no-op
    }
}
