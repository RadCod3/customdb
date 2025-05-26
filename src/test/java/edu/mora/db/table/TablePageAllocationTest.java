package edu.mora.db.table;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.storage.WALManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Inserts enough rows to force the heap file to allocate multiple pages and verifies that those pages really exist on
 * disk.
 */
class TablePageAllocationTest {

    @TempDir
    Path dir;

    @Test
    void largeInsertAllocatesPagesAndPersists() throws Exception {

        /* ── boot #1: heavy insert workload ────────────────────────── */
        DiskManager dm1 = new DiskManager(dir.toString());
        BufferPool pool1 = new BufferPool(32, dm1);
        WALManager wal1 = new WALManager(dir.toString());
        TransactionManager txm1 = new TransactionManager(wal1, pool1, dm1);
        Catalog cat1 = new Catalog(dir.toString(), pool1);

        Schema schema = new Schema(
                java.util.List.of("id", "val"),
                java.util.List.of(Schema.Type.INT, Schema.Type.STRING));

        cat1.createTable("big", schema);
        Table tbl = cat1.getTable("big");

        // ~144 B/tuple → 200 tuples ≃ 7 pages
        for (int i = 0; i < 200; i++) {
            long tx = txm1.begin();
            tbl.insertTuple(tx, txm1, new Tuple(schema, i, "x".repeat(128)));
            txm1.commit(tx);
        }

        pool1.flushAll();
        wal1.close();
        dm1.close();        // simulate clean shutdown (page count already on disk)

        /* ── open a fresh DiskManager to read real page count ──────── */
        DiskManager checkDm = new DiskManager(dir.toString());
        int pageCount = checkDm.getNumPages();
        checkDm.close();

        assertTrue(pageCount > 1,
                   "expected the heap file to have grown beyond one page but found " + pageCount);
    }
}