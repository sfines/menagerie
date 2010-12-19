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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 21-Nov-2010
 *          Time: 14:25:25
 */
public class ZooKeeperUtilsTest {

    @Test
    public void testSortBySequence() throws Exception{
        String item1 = "lock-0000001";
        String item2 = "lock-0000000";

        List<String>itemsToSort = Arrays.asList(item1,item2);

        ZkUtils.sortBySequence(itemsToSort,'-');

        assertEquals("Sort failed!",itemsToSort.get(0),item2);
        assertEquals("Sort failed!",itemsToSort.get(1),item1);
    }

    @Test
    public void testParseSequenceNumber()throws Exception{
        long testSequenceNumber = 2l;
        String itemToParse = "queue-0000002";

        long sequenceNumber = ZkUtils.parseSequenceNumber(itemToParse, '-');
        assertEquals("Parse fails!",testSequenceNumber,sequenceNumber);
    }

    @Test
    public void testFilterByPrefix()throws Exception{
        String item1 = "randomTestItem";
        String item2 = "lock-0000002";

        List<String> filteredByPrefix =
                ZkUtils.filterByPrefix(Arrays.asList(item1, item2), "lock");

        assertEquals("Filter failed to remove an item, or removed too many items!",
                            1,filteredByPrefix.size());
        assertEquals("Filter failed",item2,filteredByPrefix.get(0));
    }
}
