package edu.mora.db.table;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.storage.WALManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Inserts enough rows to overflow the first 4 KB page and checks that additional pages are allocated *and* persisted in
 * catalog.meta.
 */
class TablePageAllocationTest {

    @TempDir
    Path dir;

    @Test
    void largeInsertAllocatesPagesAndPersists() throws Exception {
        /* boot #1 – insert lots */
        DiskManager d1 = new DiskManager(dir.toString());
        BufferPool p1 = new BufferPool(32, d1);
        WALManager w1 = new WALManager(dir.toString());
        TransactionManager tm1 = new TransactionManager(w1, p1, d1);
        Catalog cat1 = new Catalog(dir.toString(), p1);

        Schema s = new Schema(
                java.util.List.of("id", "val"),
                java.util.List.of(Schema.Type.INT, Schema.Type.STRING));
        cat1.createTable("big", s);
        Table tbl = cat1.getTable("big");

        // ~140 bytes per tuple → 40–50 tuples fill a 4 KB page comfortably
        for (int i = 0; i < 200; i++) {
            long tx = tm1.begin();
            tbl.insertTuple(tx, tm1,
                            new Tuple(s, i, "x".repeat(128)));   // fixed size payload
            tm1.commit(tx);
        }
        int pagesAfterInsert = d1.getNumPages();
        assertTrue(pagesAfterInsert > 1, "should have allocated multiple pages");

        p1.flushAll();
        w1.close();
        d1.close();

        /* boot #2 – catalog should reload page list */
        DiskManager d2 = new DiskManager(dir.toString());
        BufferPool p2 = new BufferPool(32, d2);
        WALManager w2 = new WALManager(dir.toString());
        w2.recover(p2, d2);
        Catalog cat2 = new Catalog(dir.toString(), p2);

        assertEquals(1, cat2.listTables().size());
        Table reloaded = cat2.getTable("big");

        // scan should see all 200 rows immediately
        assertEquals(200, reloaded.scanAll().size());
        d2.close();
    }
}