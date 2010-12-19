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

/**
 * A Listener which is fired when certain connection events happen with respect to the
 * ZooKeeper service.
 * <p>
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 12-Dec-2010
 *          Time: 08:40:55
 */
public interface ConnectionListener {

    /**
     * Fired to indicated that the ZooKeeperService has become connected after a disconnect event
     */
    public void syncConnected();

    /**
     * Fired to indicate that the ZooKeeper Service has expired and it is necessary to re-establish watchers
     * and ephemeral nodes.
     */
    public void expired();
}
