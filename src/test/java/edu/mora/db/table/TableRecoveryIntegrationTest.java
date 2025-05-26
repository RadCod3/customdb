package edu.mora.db.table;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.executor.SimpleExecutor;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.storage.WALManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Uses SQL through SimpleExecutor to check that (1) tables function end-to-end and (2) data is still queryable after an
 * unclean crash.
 */
class TableRecoveryIntegrationTest {

    @TempDir
    Path dir;

    @Test
    void committedRowsVisibleAfterCrash() throws Exception {
        /* -------------- boot #1 ---------------- */
        DiskManager d1 = new DiskManager(dir.toString());
        BufferPool p1 = new BufferPool(32, d1);
        WALManager w1 = new WALManager(dir.toString());
        TransactionManager tx1 = new TransactionManager(w1, p1, d1);
        Catalog cat1 = new Catalog(dir.toString(), p1);
        SimpleExecutor exec1 = new SimpleExecutor(cat1, tx1);

        exec1.execute("CREATE TABLE t (id INT, name STRING)");
        exec1.execute("INSERT INTO t VALUES (1, 'one')");
        exec1.execute("INSERT INTO t VALUES (2, 'two')");
        // no graceful shutdown – simulate crash
        w1.close();
        d1.close();

        /* -------------- boot #2 (recovery) ----- */
        DiskManager d2 = new DiskManager(dir.toString());
        BufferPool p2 = new BufferPool(32, d2);
        WALManager w2 = new WALManager(dir.toString());
        w2.recover(p2, d2);
        TransactionManager tx2 = new TransactionManager(w2, p2, d2);
        Catalog cat2 = new Catalog(dir.toString(), p2);
        SimpleExecutor exec2 = new SimpleExecutor(cat2, tx2);

        List<String> out = new ArrayList<>();
        exec2.execute("SELECT * FROM t", t -> out.add(t.toString()));

        assertEquals(2, out.size());
        assertTrue(out.get(0).contains("one"));
        assertTrue(out.get(1).contains("two"));

        // prove table still usable
        exec2.execute("INSERT INTO t VALUES (3, 'three')");
        out.clear();
        exec2.execute("SELECT * FROM t", t -> out.add(t.toString()));
        assertEquals(3, out.size());

        w2.close();
        d2.close();
    }
}