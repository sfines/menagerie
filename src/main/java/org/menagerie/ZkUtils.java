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

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Static utility class for manipulating ZooKeeper-related strings.
 * <p>
 *
 * @author Scott Fines
 * @version 1.0
 */
public class ZkUtils {

    /*Can't instantiate me!*/
    private ZkUtils(){}

    /**
     * Sorts a list of ZooKeeper-nodes based on the sequential pattern set by ZooKeeper, which is
     * {@code (prefix)(delimiter)(sequence number)}.
     * <p>
     * When this method completes, {@code items} will be sorted according to the sequence number, with the
     * lowest sequence number occurring first.
     *
     * @param items the items to sort
     * @param sequenceDelimiter the delimiter separating prefix elements from sequence numbers
     */
    public static void sortBySequence(List<String> items,char sequenceDelimiter){
        Collections.sort(items,new SequenceComparator(sequenceDelimiter));
    }

    /**
     * Sorts a list of ZooKeeper-nodes based on the sequential pattern set by ZooKeeper, which is
     * {@code (prefix)(delimiter)(sequence number)}, in reverse order.
     * <p>
     * When this method completes, {@code items} will be sorted according to the sequence number, with the
     * highest sequence number occurring first.
     *
     * @param items the items to sort
     * @param sequenceDelimiter the delimiter separating prefix elements from sequence numbers
     */
    public static void sortByReverseSequence(List<String> items,char sequenceDelimiter){
        Collections.sort(items,Collections.reverseOrder(new SequenceComparator(sequenceDelimiter)));
    }

    /**
     * Parses a sequence number from a ZooKeeper sequential node-name.
     *
     * @param node the node to parse
     * @param sequenceStartDelimiter the delimiter separating a node prefix from its sequence number
     * @return the sequence number of the given node
     */
    public static int  parseSequenceNumber(String node,char sequenceStartDelimiter){
        String sequenceStr = parseSequenceString(node,sequenceStartDelimiter);
        return Integer.parseInt(sequenceStr);
    }

    /**
     * Parses a sequence number as a String from a ZooKeeper sequential node-name.
     *
     * @param node the node to parse
     * @param sequenceStartDelimiter the delimiter separating a node prefix from its sequence number
     * @return the sequence number of the given node
     */
    public static String parseSequenceString(String node, char sequenceStartDelimiter){
        if(node==null)throw new NullPointerException("No node specified!");

        int seqStartIndex = node.lastIndexOf(sequenceStartDelimiter);
        if(seqStartIndex<0)
            throw new IllegalArgumentException("No sequence is parseable from the specified node: " +
                                                "Node= <"+node+">, sequence delimiter=<"+sequenceStartDelimiter+">");

        return node.substring(seqStartIndex+1);
    }

    /**
     * Filters the given node list by the given prefixes.
     * <p>
     * This method is all-inclusive--if any element in the node list starts with any of the given prefixes, then it
     * is included in the result.
     *
     * @param nodes the nodes to filter
     * @param prefixes the prefixes to include in the result
     * @return a list of elements where every element starts with one of the prefixes specified.
     */
    public static List<String> filterByPrefix(List<String> nodes,String... prefixes) {
        List<String> lockChildren = new ArrayList<String>();
        for(String child:nodes){
            for(String prefix:prefixes){
                if(child.startsWith(prefix)){
                    lockChildren.add(child);
                    break;
                }
            }
        }
        return lockChildren;
    }

    /**
     * Deletes an element from ZooKeeper safely.
     * <p>
     * If the element has already been deleted from ZooKeeper, then a NoNode Exception is normally thrown by ZooKeeper.
     * This method suppresses those NoNode exceptions .
     *
     * @param zk the ZooKeeper client to use
     * @param nodeToDelete the node to delete
     * @param version the version of that node to remove
     * @return true if the node was deleted
     * @throws KeeperException if some issue with the ZooKeeper server occurs
     * @throws InterruptedException if some communication error happens
     *                              between the ZooKeeper client and the ZooKeeper service
     */
    public static boolean safeDelete(ZooKeeper zk, String nodeToDelete, int version)throws KeeperException, InterruptedException{
        try {
            zk.delete(nodeToDelete,version);
            return true;
        } catch (KeeperException ke) {
            //if the node has already been deleted, don't worry about it
            if(ke.code()!=KeeperException.Code.NONODE)
                throw ke;
            else
                return false;
        }
    }

    /**
     * Deletes all the listed elements from ZooKeeper safely.
     * <p>
     * If any element has already been deleted from ZooKeeper, then a NoNode Exception is normally thrown by ZooKeeper.
     * This method suppresses those exceptions, while allowing through all other kinds.
     * <p>
     * If ZooKeeper Exception which is <i>not</i> of the type NoNode, then it will be thrown immediately, and any
     * elements which had not yet been deleted will not be deleted by this method.
     *
     * @param zk the ZooKeeper client to use
     * @param nodesToDelete the nodes to delete
     * @param version the version of the nodes to remove
     * @throws KeeperException if some issue with the ZooKeeper server which is <i>not</i> of type NoNode occurs.
     * @throws InterruptedException if some communication error happens between the ZooKeeper client and server
     */
    public static void safeDeleteAll(ZooKeeper zk, int version, String... nodesToDelete) throws KeeperException, InterruptedException {
        for(String permitNode:nodesToDelete){
            ZkUtils.safeDelete(zk,permitNode,version);
        }
    }

     /**
     * Deletes all the listed elements from ZooKeeper safely.
     * <p>
     * If any element has already been deleted from ZooKeeper, then a NoNode Exception is normally thrown by ZooKeeper.
     * This method suppresses those exceptions, while allowing through all other kinds.
     * <p>
     * If ZooKeeper Exception which is <i>not</i> of the type NoNode, then it will be thrown immediately, and any
     * elements (such as additional children) which had not yet been deleted will not be deleted by this method.
     *
     * @param zk the ZooKeeper client to use
     * @param nodeToDelete the node to recursively delete
     * @param version the version of the nodes to remove
     * @throws KeeperException if some issue with the ZooKeeper server which is <i>not</i> of type NoNode occurs.
     * @throws InterruptedException if some communication error happens between the ZooKeeper client and server
      */
     public static void recursiveSafeDelete(ZooKeeper zk, String nodeToDelete, int version) throws KeeperException, InterruptedException{
         try{
             List<String> children = zk.getChildren(nodeToDelete,false);
             for(String child: children){
                 recursiveSafeDelete(zk,nodeToDelete+"/"+child,version);
             }
             //delete this node
             safeDelete(zk,nodeToDelete,version);
         }catch(KeeperException ke){
             if(ke.code()!=KeeperException.Code.NONODE)
                 throw ke;
         }
    }

    /**
     * Creates a new node safely.
     * <p>
     * If a node already exists, ZooKeeper may throw a KeeperException of type {@code KeeperException.Code.NODEEXISTS}.
     * This method suppresses that exception, allow creates of potentially contentious nodes to remain atomic.
     * <p>
     * If the node already exists, the data will <i>not</i> be set to that node. This reflects the fact that a
     * create-and-set operation is not atomic, and may result in a data race. Instead, this method will quietly return.
     * <p>
     * If any other type of KeeperException is thrown, that exception will be propogated up the stack.
     * <p>
     * Note: If the CreateMode of the node of interest is sequential, then this method is unnecessary, as ZooKeeper
     * will never throw a NODEEXISTS exception for modes of those types.
     *
     * @param zk the ZooKeeper client to use
     * @param nodeToCreate the node to create
     * @param data the data to set on the newly created node.
     * @param privileges the privileges to associate with that node
     * @param createMode the Mode that node will possess
     * @return the name of the newly qualified node
     * @throws KeeperException if an Error occurs on the ZooKeeper server which is <i>not</i> of type NODEEXISTS
     * @throws InterruptedException if a communication error between the client and server occurs.
     */
    public static String safeCreate(ZooKeeper zk, String nodeToCreate,byte[] data, List<ACL> privileges, CreateMode createMode) throws KeeperException,InterruptedException {
        try {
            return zk.create(nodeToCreate,data,privileges,createMode);
        } catch (KeeperException ke) {
            //if the node has already been created, don't worry about it
            if(ke.code()!=KeeperException.Code.NODEEXISTS)
                throw ke;
            else{
                /*
                This only happens if {@code createMode} is EPHEMERAL or PERSISTENT--sequential modes do not throw
                this error code.

                In the case that we are looking for EPHEMERAL or PERSISTENT, then we already know the full path
                of the node we were trying to create, so just return that, since it already exists.
                */
                return nodeToCreate;
            }

        }
    }

    /**
     * Attempts to get data from ZooKeeper in a single operation.
     * <p>
     *  Because it is possible that the node under request does not exist, it is possible that
     * a KeeperException.NONODE exception will be thrown. This method swallows
     * NONODE exceptions and returns an empty byte[] when that node does not exist.
     *
     * @param zk the ZooKeeper client
     * @param node the node to get data for
     * @param watcher any watcher to attach to this node, if it exists
     * @param stat a stat object for use in the getData call
     * @return the byte[] information contained in that node, if that node exists. If {@code node} does not exist,
     *          then this returns an empty byte[]
     *
     * @throws InterruptedException if communication between the ZooKeeper client and servers are broken
     * @throws KeeperException if there is a problem getting Data from the ZooKeeper server that <i>doesn't</i> have
     *          the error code of NONODE
     */
    public static byte[] safeGetData(ZooKeeper zk, String node, Watcher watcher, Stat stat) throws InterruptedException, KeeperException {
        try {
            return zk.getData(node,watcher,stat);
        } catch (KeeperException e) {
            if(e.code()!=KeeperException.Code.NONODE)
                throw e;
            else
                return new byte[]{};
        }
    }

    /**
     * Attempts to get data from ZooKeeper in a single operation. Because it is possible that the node under request
     * does not exist, it is possible that a KeeperException.NONODE exception will be thrown. This method swallows
     * NONODE exceptions and returns an empty byte[] when that node does not exist.
     *
     * @param zk the ZooKeeper client
     * @param node the node to get data for
     * @param watch true if the default watcher is to be attached to this node's data
     * @param stat a stat object for use in the getData call
     * @return the byte[] information contained in that node, if that node exists. If {@code node} does not exist,
     *          then this returns an empty byte[]
     *
     * @throws InterruptedException if communication between the ZooKeeper client and servers are broken
     * @throws KeeperException if there is a problem getting Data from the ZooKeeper server that <i>doesn't</i> have
     *          the error code of NONODE
     */
    public static byte[] safeGetData(ZooKeeper zk, String node, boolean watch, Stat stat) throws InterruptedException, KeeperException {
        try {
            return zk.getData(node,watch,stat);
        } catch (KeeperException e) {
            if(e.code()!=KeeperException.Code.NONODE)
                throw e;
            else
                return new byte[]{};
        }
    }

    public static String recursiveSafeCreate(ZooKeeper zk, String node,byte[] data,List<ACL> privileges,CreateMode createMode) throws KeeperException,InterruptedException{
        if(node==null||node.length()<0)return node; //nothing to do
        else if("/".equals(node))return node; //can't create any further
        else{
            int index = node.lastIndexOf("/");
            if(index==-1) return node; //nothing to do
            String parent = node.substring(0,index);
            //make sure that the parent has been created
            recursiveSafeCreate(zk,parent,data,privileges,createMode);

            //create this node now
            return safeCreate(zk,node,data,privileges,createMode);
        }
    }

/*--------------------------------------------------------------------------------------------------------------------*/
    /*private helper classes*/
    private static class SequenceComparator implements Comparator<String>{
        private final char sequenceDelimiter;

        public SequenceComparator(char sequenceDelimiter) {
            this.sequenceDelimiter = sequenceDelimiter;
        }

        @Override
        public int compare(String child1, String child2) {
            long childOneSeqNbr = ZkUtils.parseSequenceNumber(child1,sequenceDelimiter);
            long childTwoSeqNbr = ZkUtils.parseSequenceNumber(child2,sequenceDelimiter);

            if(childOneSeqNbr<childTwoSeqNbr)return -1;
            else if(childOneSeqNbr>childTwoSeqNbr)return 1;
            else return 0;
        }
    }
}
