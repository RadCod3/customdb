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

class UpdateDeleteRecoveryIntegrationTest {

    @TempDir
    Path dir;

    @Test
    void updateAndDeleteRedoneOrUndoneProperly() throws Exception {

        /* ─── boot #1: create, update, delete, CRASH ─── */
        DiskManager d1 = new DiskManager(dir.toString());
        BufferPool p1 = new BufferPool(32, d1);
        WALManager w1 = new WALManager(dir.toString());
        TransactionManager tx1 = new TransactionManager(w1, p1, d1);
        Catalog cat1 = new Catalog(dir.toString(), p1);
        SimpleExecutor e1 = new SimpleExecutor(cat1, tx1);

        e1.execute("CREATE TABLE t (id INT, name STRING)");
        e1.execute("INSERT INTO t VALUES (1, 'foo')");
        e1.execute("INSERT INTO t VALUES (2, 'bar')");

        e1.execute("UPDATE t SET name = 'FOO' WHERE id = 1");
        e1.execute("DELETE FROM t WHERE id = 2");

        // crash: no flush
        w1.close();
        d1.close();

        /* ─── boot #2: recovery ─── */
        DiskManager d2 = new DiskManager(dir.toString());
        BufferPool p2 = new BufferPool(32, d2);
        WALManager w2 = new WALManager(dir.toString());
        w2.recover(p2, d2);
        TransactionManager tx2 = new TransactionManager(w2, p2, d2);
        Catalog cat2 = new Catalog(dir.toString(), p2);
        SimpleExecutor e2 = new SimpleExecutor(cat2, tx2);

        List<String> rows = new ArrayList<>();
        e2.execute("SELECT * FROM t", t -> rows.add(t.toString()));

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).contains("FOO"));
    }
}