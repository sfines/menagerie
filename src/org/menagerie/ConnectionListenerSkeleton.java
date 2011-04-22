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

/**
 * Abstract Skeleton implementation of a ConnectionListener interface.
 *
 * <p>All methods in the ConnectionListener are implemented as no-ops, and it is the choice
 * of the concrete implementation to decide which actions to respond to, and which to ignore.
 *
 * @author Scott Fines
 *         Date: Apr 22, 2011
 *         Time: 9:26:20 AM
 */
public abstract class ConnectionListenerSkeleton implements ConnectionListener{

    @Override
    public void syncConnected() {
        //default no-op
    }

    @Override
    public void expired() {
        //default no-op
    }

    @Override
    public void disconnected() {
        //default no-op
    }
}
