package org.menagerie.collections;

import org.apache.zookeeper.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.menagerie.*;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Tests to assert that ZkHashMap is correct under single-threaded access.
 * <p>
 * Multi-threaded and Multi-client tests will be executed in separate test modules, for clarity
 *
 * //TODO -sf- document the location of the Multi-threaded and Multi-client test modules
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 08-Jan-2011
 *          Time: 10:43:03
 */
public class ZkHashMapTestSingleThreaded {
    private static final String hostString = "localhost:2181";
    private static final String baseLockPath = "/test-maps";
    private static final int timeout = 2000;

    private static ZooKeeper zk;
    private static ZkSessionManager zkSessionManager;
    private ZkHashMap<String,String> testMap;

    @Before
    public void setup() throws Exception {
        zk = newZooKeeper();

        //be sure that the lock-place is created

        zk.create(baseLockPath,new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        zkSessionManager = new BaseZkSessionManager(zk);

        testMap = new ZkHashMap<String, String>(baseLockPath,zkSessionManager,new JavaEntrySerializer<String, String>());

        
    }

    @After
    public void tearDown() throws Exception{
        try{
           ZkUtils.recursiveSafeDelete(zk,baseLockPath,-1);

        }catch(KeeperException ke){
            //suppress because who cares what went wrong after our tests did their thing?
        }finally{
            zk.close();
        }
    }

    @Test(timeout = 1000l)
    public void testConstructorWontAddNewBuckets() throws Exception{
        int numBucketsNow = ZkUtils.filterByPrefix(zk.getChildren(baseLockPath,false),"bucket").size();

        @SuppressWarnings({"UnusedDeclaration"})
        ZkHashMap<String,String> type = new ZkHashMap<String, String>(baseLockPath,zkSessionManager,new JavaEntrySerializer<String, String>());

        int newNumBuckets = ZkUtils.filterByPrefix(zk.getChildren(baseLockPath,false),"bucket").size();
        assertEquals("Buckets were added or removed incorrectly!",numBucketsNow,newNumBuckets);
    }

    @Test(timeout = 1000l)
    public void testPutIfAbsentAbsent() throws Exception{
        String putValue = testMap.putIfAbsent("Test Key", "Test Value");
        assertEquals("Incorrect return value for putIfAbsent","Test Value",putValue);
        assertEquals("putIfAbsent put incorrect value in place","Test Value",testMap.get("Test Key"));
    }

    @Test(timeout = 1000l)
    public void testPutIfAbsentNotAbsent() throws Exception{
        testMap.put("Test Key","Test Value");

        String putValue = testMap.putIfAbsent("Test Key", "Test Value 2");
        assertEquals("Incorrect return value for putIfAbsent","Test Value",putValue);
        assertEquals("putIfAbsent put incorrect value in place","Test Value",testMap.get("Test Key"));
    }

    @Test(timeout = 1000l)
    public void testRemoveIfMatchesMatches() throws Exception{
        testMap.put("Test Key","Test Value");

        boolean removed = testMap.remove("Test Key", "Test Value");
        assertTrue("Element reported unremoved!",removed);

        String value = testMap.get("Test Key");
        assertNull("Element was not removed!",value);
    }

    @Test(timeout = 1000l)
    public void testRemoveIfMatchesNoMatch() throws Exception{
        testMap.put("Test Key","Test Value");

        boolean removed = testMap.remove("Test Key", "Test Value 2");
        assertTrue("Element reported removed!",!removed);

        String value = testMap.get("Test Key");
        assertEquals("Element was removed!","Test Value",value);
    }

    @Test(timeout = 1000l)
    public void testRemoveIfMatchesNoKey() throws Exception{
        boolean removed = testMap.remove("Test Key", "Test Value 2");
        assertTrue("Element reported removed!",!removed);

        String value = testMap.get("Test Key");
        assertNull("Element was removed",value);
    }

    @Test(timeout = 1000l)
    public void testReplaceIfMatchesMatches() throws Exception{
        String value = testMap.put("Test Key", "Test Value");
        assertEquals("Value was not put!","Test Value",value);

        boolean replaced = testMap.replace("Test Key", "Test Value", "New Test Value");
        assertTrue("Element was not reported replaced!",replaced);

        String testVal = testMap.get("Test Key");
        assertEquals("Element was not replaced!","New Test Value",testVal);
    }

    @Test(timeout = 1000l)
    public void testReplaceIfMatchesNoMatch() throws Exception{
        String value = testMap.put("Test Key", "Test Value");
        assertEquals("Value was not put!","Test Value",value);

        boolean replaced = testMap.replace("Test Key", "Test Value 2", "New Test Value");
        assertTrue("Element was reported replaced!",!replaced);

        String testVal = testMap.get("Test Key");
        assertEquals("Element was not replaced!","Test Value",testVal);
    }

    @Test(timeout = 1000l)
    public void testReplaceIfNoKey() throws Exception{
        boolean replaced = testMap.replace("Test Key", "Test Value 2", "New Test Value");
        assertTrue("Element was reported replaced!",!replaced);

        String testVal = testMap.get("Test Key");
        assertNull("Element was not replaced!",testVal);
    }

    @Test(timeout = 1000l)
    public void testReplace() throws Exception{
        String value = testMap.put("Test Key", "Test Value");
        assertEquals("Value was not put!","Test Value",value);

        String newVal = testMap.replace("Test Key", "New Test Value");
        assertEquals("replace returned incorrect value!","New Test Value",newVal);

        String getVal = testMap.get("Test Key");
        assertEquals("get returned incorrect value!","New Test Value",getVal);
    }

    @Test(timeout = 1000l)
    public void testReplaceNoKey() throws Exception{
        String newVal = testMap.replace("Test Key", "New Test Value");
        assertNull("A value is reported associated with this key!",newVal);

        String getVal = testMap.get("Test Key");
        assertNull("A value is associated with this key!",getVal);
    }

    @Test(timeout = 1000l, expected = NullPointerException.class)
    public void testReplaceNoValue() throws Exception{
        testMap.replace("Test Key",null);
    }

    @Test(timeout = 1000l)
    public void testSize() throws Exception{
        int startSize = testMap.size();
        assertEquals("test map reports incorrect size!",0, startSize);

        String val = testMap.put("Test Key", "Test Value");
        assertEquals("Value did not get inserted correctly!","Test Value", val);

        int newSize = testMap.size();
        assertEquals("Test map reports incorrect size!",1,newSize);
    }

    @Test(timeout = 1000l)
    public void testIsEmpty() throws Exception{
        boolean empty = testMap.isEmpty();
        assertTrue("map reports itself nonempty!",empty);

        String val = testMap.put("Test Key", "Test Value");
        assertEquals("Value did not get inserted correctly!","Test Value", val);

        boolean empty2 = testMap.isEmpty();
        assertTrue("map reports itself empty!",!empty2);
    }

    @Test(timeout = 1000l)
    public void testContainsKey() throws Exception{
        boolean doesNotContainKey = !testMap.containsKey("Test Key");
        assertTrue("map claims to contain key!",doesNotContainKey);

        String val = testMap.put("Test Key", "Test Value");
        assertEquals("Value did not get inserted correctly!","Test Value", val);

        boolean containsKey = testMap.containsKey("Test Key");
        assertTrue("map claims not to contain key!",containsKey);
    }

    @Test(timeout = 1000l, expected = NullPointerException.class)
    public void testContainsNullKeyExplodes() throws Exception{
        testMap.containsKey(null);
    }

    @Test(timeout = 1000l)
    public void testContainsValue() throws Exception{
        boolean doesNotContainValue = !testMap.containsValue("Test Value");
        assertTrue("map claims to contain value!",doesNotContainValue);

        insertAndAssert("Test Key","Test Value");

        boolean containsValue = testMap.containsValue("Test Value");
        assertTrue("map claims not to contain value!",containsValue);
    }

    @Test(timeout = 1000l)
    public void testRemoveByKey() throws Exception{
        String shouldBeNull = testMap.remove("Test Key");
        assertNull("map returned a removed value!",shouldBeNull);

        insertAndAssert("Test Key", "Test Value");

        String removedValue = testMap.remove("Test Key");
        assertEquals("map returned incorrect value","Test Value",removedValue);

        boolean shouldNotContain = !testMap.containsKey("Test Key");
        assertTrue("map still claims to contain key!",shouldNotContain);
    }

    @Test(timeout = 1000l, expected = NullPointerException.class)
    public void testRemoveByNullKey() throws Exception{
        testMap.remove(null);
    }

    @Test(timeout = 1000l)
    public void testClear() throws Exception{
        insertAndAssert("Test Key","Test Value");

        assertEquals("Map reporting zero size",1,testMap.size());

        testMap.clear();
        assertEquals("Map reporting nonzero size",0,testMap.size());

        String shouldBeNull = testMap.get("Test Key");
        assertNull("map still contains entry",shouldBeNull);
    }

    @Test(timeout = 1000l)
    public void testRemoveEntryFromEntrySet() throws Exception{
        //put some entries into the map
        for(int i=0;i<5;i++){
            testMap.put(Integer.toString(i),"Test Value "+ i);
        }
        assertEquals("Test map incorrectly reports size!",5,testMap.size());

        Set<Map.Entry<String,String>> entries = testMap.entrySet();
        Iterator<Map.Entry<String,String>> entryIterator = entries.iterator();

        int size = 5;
        while(entryIterator.hasNext()){
            Map.Entry<String,String> entry = entryIterator.next();
            System.out.println(Thread.currentThread().getName()+": reading entry "+ entry);
            if(Integer.parseInt(entry.getKey())%2==0){
                System.out.println(Thread.currentThread().getName()+ ": Removing entry "+ entry);
                //call remove, reassert size, and check that that entry is no longer in the entries set, or in the map
                entryIterator.remove();
                size--;
                assertEquals("Deletion not recorded!",size, testMap.size());
                
                //now check contains
                boolean shouldNotContainKey = !testMap.containsKey(entry.getKey());
                assertTrue("Key " + entry.getKey()+" is still in the map!", shouldNotContainKey);
                
                boolean shouldNotContainEntry = !entries.contains(entry);
                assertTrue("Entry " + entry+" is still contained!",shouldNotContainEntry);
            }
        }
        //make sure that the size is half of what you started with
        assertEquals("Reported size does not match after removal!",5/2,testMap.size());
    }

    @Test(timeout = 1000l)
    public void testRemoveKeyFromKeySet() throws Exception{
        //put some entries into the map
        for(int i=0;i<5;i++){
            testMap.put(Integer.toString(i),"Test Value "+ i);
        }
        assertEquals("Test map incorrectly reports size!",5,testMap.size());

        Set<String> keys = testMap.keySet();
        Iterator<String> keyIterator = keys.iterator();

        int size = 5;
        while(keyIterator.hasNext()){
            String key = keyIterator.next();
            System.out.println(Thread.currentThread().getName()+": reading key "+ key);
            if(Integer.parseInt(key)%2==0){
                System.out.println(Thread.currentThread().getName()+ ": Removing key "+ key);
                //call remove, reassert size, and check that that entry is no longer in the entries set, or in the map
                keyIterator.remove();
                size--;
                assertEquals("Deletion not recorded!",size, testMap.size());

                //now check contains
                boolean shouldNotContainKey = !testMap.containsKey(key);
                assertTrue("Key " + key+" is still in the map!", shouldNotContainKey);

                boolean shouldNotContainEntry = !keys.contains(key);
                assertTrue("Key " + key+" is still contained in the key set!",shouldNotContainEntry);
            }
        }
        //make sure that the size is half of what you started with
        assertEquals("Reported size does not match after removal!",5/2,testMap.size());
    }

    private void insertAndAssert(String key, String value){
        String val = testMap.put(key, value);
        assertEquals("Value did not get inserted correctly!",value, val);
    }

    private static ZooKeeper newZooKeeper() throws IOException {
        return new ZooKeeper(hostString, timeout,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        });
    }
}

