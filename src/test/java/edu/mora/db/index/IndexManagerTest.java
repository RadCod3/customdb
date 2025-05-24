package edu.mora.db.index;

import edu.mora.db.storage.RecordId;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the IndexManager in edu.mora.db.index.
 */
public class IndexManagerTest {
    @Test
    void testCreateAndSearchSingleIndex() {
        IndexManager mgr = new IndexManager();
        mgr.createIndex("idx", 3, Integer.class);
        assertTrue(mgr.search("idx", 1).isEmpty(), "New index should be empty");

        RecordId rid = new RecordId(7, 21);
        mgr.insert("idx", 7, rid);
        List<RecordId> out = mgr.search("idx", 7);
        assertEquals(1, out.size());
        assertEquals(7, out.get(0).getPageId());
        assertEquals(21, out.get(0).getOffset());
    }

    @Test
    void testMultipleIndexesAreIsolated() {
        IndexManager mgr = new IndexManager();
        mgr.createIndex("A", 3, Integer.class);
        mgr.createIndex("B", 3, Integer.class);

        RecordId rA = new RecordId(1, 1);
        RecordId rB = new RecordId(2, 2);
        mgr.insert("A", 5, rA);
        mgr.insert("B", 5, rB);

        List<RecordId> resA = mgr.search("A", 5);
        List<RecordId> resB = mgr.search("B", 5);
        assertEquals(1, resA.size());
        assertEquals(1, resB.size());
        assertEquals(1, resA.get(0).getPageId());
        assertEquals(2, resB.get(0).getPageId());
    }
}
