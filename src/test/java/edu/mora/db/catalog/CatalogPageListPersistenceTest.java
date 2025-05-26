package edu.mora.db.catalog;

import edu.mora.db.executor.SimpleExecutor;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.storage.WALManager;
import edu.mora.db.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Inserts a row, simulates a crash (without flushing BufferPool), restarts the whole stack, and checks that the row is
 * still visible.
 * <p>
 * This proves that Catalog correctly persisted the page-id list and that Table.flush/scan works immediately after
 * recovery.
 */
class CatalogPageListPersistenceTest {

    @TempDir
    Path dir;

    @Test
    void pageOwnershipPersistsAcrossRestart() throws Exception {

        /* ======== boot #1: create + insert ========== */
        DiskManager d1 = new DiskManager(dir.toString());
        BufferPool p1 = new BufferPool(32, d1);
        WALManager w1 = new WALManager(dir.toString());
        w1.recover(p1, d1);                       // no-op first boot
        TransactionManager txm1 = new TransactionManager(w1, p1, d1);
        Catalog c1 = new Catalog(dir.toString(), p1);
        SimpleExecutor e1 = new SimpleExecutor(c1, txm1);

        // DDL
        e1.execute("CREATE TABLE t (id INT, name STRING)");
        e1.execute("INSERT INTO t VALUES (1, 'foo')");

        // DO NOT call p1.flushAll() – mimic sudden crash
        w1.close();
        d1.close();

        /* ======== boot #2: recovery ========== */
        DiskManager d2 = new DiskManager(dir.toString());
        BufferPool p2 = new BufferPool(32, d2);
        WALManager w2 = new WALManager(dir.toString());
        w2.recover(p2, d2);                       // redo committed work
        TransactionManager txm2 = new TransactionManager(w2, p2, d2);
        Catalog c2 = new Catalog(dir.toString(), p2);
        SimpleExecutor e2 = new SimpleExecutor(c2, txm2);

        List<Tuple> rows = new ArrayList<>();
        e2.execute("SELECT * FROM t", rows::add);

        assertEquals(1, rows.size(), "row inserted before crash should be visible");
        assertEquals(1, rows.get(0).getField(0));
        assertEquals("foo", rows.get(0).getField(1));

        p2.flushAll();
        w2.close();
        d2.close();
    }
}