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

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;

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
    public static long parseSequenceNumber(String node,char sequenceStartDelimiter){
        if(node==null)throw new NullPointerException("No node specified!");

        int seqStartIndex = node.lastIndexOf(sequenceStartDelimiter);
        if(seqStartIndex<0)
            throw new IllegalArgumentException("No sequence is parseable from the specified node: " +
                                                "Node= <"+node+">, sequence delimiter=<"+sequenceStartDelimiter+">");

        String sequenceStr = node.substring(seqStartIndex+1);
        return Long.parseLong(sequenceStr);
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
     * @throws KeeperException if some issue with the ZooKeeper server occurs
     * @throws InterruptedException if some communication error happens
     *                              between the ZooKeeper client and the ZooKeeper service
     */
    public static void safeDelete(ZooKeeper zk, String nodeToDelete, int version)throws KeeperException, InterruptedException{
        try {
            zk.delete(nodeToDelete,version);
        } catch (KeeperException ke) {
            //if the node has already been deleted, don't worry about it
            if(ke.code()!=KeeperException.Code.NONODE)
                throw ke;
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
