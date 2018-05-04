package ru.csc.bdse.kv.partitioned;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import ru.csc.bdse.kv.KeyValueApi;

import java.util.Optional;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test have to be implemented
 *
 * @author alesavin
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class AbstractPartitionedKeyValueApiHttpClientTest {

    private static final double PROPORTION_EPS = 1e-8;

    protected abstract KeyValueApi newCluster1();

    protected abstract KeyValueApi newCluster2();

    protected abstract Set<String> keys();

    protected abstract float expectedKeysLossProportion();

    protected abstract float expectedUndeletedKeysProportion();

    private KeyValueApi cluster1 = newCluster1();
    private KeyValueApi cluster2 = newCluster2();

    private Set<String> keys = keys();


    @Test
    public void test1Put1000KeysAndReadItCorrectlyOnCluster1() {
        for (String key : keys) {
            cluster1.put(key, key.getBytes());
        }
        for (String key : keys) {
            Optional<byte[]> val = cluster1.get(key);
            assertTrue(val.isPresent());
            assertArrayEquals(key.getBytes(), val.get());
        }
    }

    @Test
    public void test2ReadKeysFromCluster2AndCheckLossProportion() {
        int lossCnt = 0;
        for (String key : keys) {
            if (!cluster2.get(key).isPresent()) {
                lossCnt++;
            }
        }
        assertEquals(expectedKeysLossProportion(),  (float) lossCnt / keys.size(), PROPORTION_EPS);
    }

    @Test
    public void test3DeleteAllKeysFromCluster2() {
        for (String key : keys) {
            cluster2.delete(key);
        }
        for (String key : keys) {
            assertFalse(cluster2.get(key).isPresent());
        }
    }

    @Test
    public void test4ReadKeysFromCluster1AfterDeletionAtCluster2() {
        Set<String> currentKeys = cluster1.getKeys("");
        assertEquals(expectedUndeletedKeysProportion(), (float) currentKeys.size() / keys.size(), PROPORTION_EPS);
    }
}


