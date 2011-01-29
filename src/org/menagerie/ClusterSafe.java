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

import java.lang.annotation.*;

/**
 * Indicates that a given class is safe to be used concurrently by many parties in a Cluster.
 *
 * <p>Cluster-safety is defined around the concept of <i>parties</i>. a <i>Party</i> is defined as both a Thread and
 * a surrounding system. This definition is somewhat dependent on what distributed system is being used. For example,
 * a party in ZooKeeper consists of both the execution Thread and the ZooKeeper client session being used. Different
 * client sessions constitute different parties, but so do different threads using a shared ZooKeeper client.
 *
 * <p> An action (any block of executable code) which is cluster-safe is any action which will behave correctly when
 * performed by multiple parties, regardless of the execution order of those parties, and without additional
 * synchronization on the part of the action caller.
 *
 * <p>This definition requires the caller to understand two important issues: execution order across the system, and
 * the concept of correctness.
 *
 * <p>Generally, an action is considered correct if it does what it promises to do ("adheres to its specification").
 * Since this definition is somewhat loose, correctness is somewhat vague and highly system-specific.
 *
 * <p> Most systems can determine some form of ordering which is absolute across all parties in a cluster,
 * either through the use of <a href=http://en.wikipedia.org/wiki/Vector_clock >Vector Clocks</a>
 * or other form of peer-to-peer agreement mechanism, or via a master ordering service.
 * For example, ZooKeeper uses a leader-election based strategy to determine an absolute ordering.
 * Distributed systems which cannot guarantee an absolute ordering cannot  guarantee cluster safety
 * under this definition, and  the user should be careful to understand what
 * conditions can be guaranteed by those systems.
 *
 * <p> An action which is cluster-safe requires that any parties which operate this action must be able to trust in
 * its safety; since parties consist of a thread plus a system-specific distributed component, this requires that
 * multiple threads must be able to trust in that action's safety as well. Therefore, any action which is cluster safe
 * is necessarily thread-safe as well.
 *
 * <p> Any class which contains <i>only</i> cluster-safe methods is considered to be cluster-safe as w
 * ell.
 * @author Scott Fines
 * @version 1.0
 *          Date: 29-Jan-2011
 *          Time: 13:19:11
 */
@Documented
@Target({ElementType.TYPE,ElementType.METHOD})
@Retention(RetentionPolicy.SOURCE)
public @interface ClusterSafe {
}
