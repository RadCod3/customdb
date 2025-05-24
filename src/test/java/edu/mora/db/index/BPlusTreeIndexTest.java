package edu.mora.db.index;

import edu.mora.db.storage.RecordId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the in-memory BPlusTreeIndex implementation.
 */
public class BPlusTreeIndexTest {
    private BPlusTreeIndex<Integer> tree;

    @BeforeEach
    void setUp() {
        tree = new BPlusTreeIndex<>(3);
    }

    @Test
    void testSingleInsertAndSearch() {
        // Tree should start empty
        assertTrue(tree.search(10).isEmpty(), "Search in an empty tree should be empty");

        // After one insert, exactly one record should be found
        RecordId rid = new RecordId(1, 0);
        tree.insert(10, rid);
        List<RecordId> results = tree.search(10);
        assertEquals(1, results.size(), "Should find exactly one record");
        assertEquals(1, results.get(0).getPageId());
        assertEquals(0, results.get(0).getOffset());
    }

    @Test
    void testSearchNonExistentKey() {
        assertTrue(tree.search(999).isEmpty(), "Non-existent key should yield empty list");
    }

    @Test
    void testDuplicateKeyInsert() {
        RecordId r1 = new RecordId(1, 5);
        RecordId r2 = new RecordId(2, 10);
        tree.insert(42, r1);
        tree.insert(42, r2);

        List<RecordId> results = tree.search(42);
        assertEquals(2, results.size(), "Should return two record IDs for duplicate key");
        assertEquals(1, results.get(0).getPageId());
        assertEquals(5, results.get(0).getOffset());
        assertEquals(2, results.get(1).getPageId());
        assertEquals(10, results.get(1).getOffset());
    }

    @Test
    void testLeafNodeSplitKeepsAllKeys() {
        for (int i = 1; i <= 6; i++) {
            tree.insert(i, new RecordId(i, i * 100));
        }
        for (int i = 1; i <= 6; i++) {
            List<RecordId> res = tree.search(i);
            assertEquals(1, res.size(), "Key " + i + " should be present");
            assertEquals(i, res.get(0).getPageId());
            assertEquals(i * 100, res.get(0).getOffset());
        }
    }

    @Test
    void testRootSplitCreatesInternalNode() {
        BPlusTreeIndex<Integer> smallTree = new BPlusTreeIndex<>(2);
        for (int i = 1; i <= 8; i++) {
            smallTree.insert(i, new RecordId(i, i + 50));
        }
        assertFalse(smallTree.root.isLeaf(), "Root should be internal after splits");
        for (int i = 1; i <= 8; i++) {
            List<RecordId> res = smallTree.search(i);
            assertEquals(1, res.size());
            assertEquals(i + 50, res.get(0).getOffset());
        }
    }
}

