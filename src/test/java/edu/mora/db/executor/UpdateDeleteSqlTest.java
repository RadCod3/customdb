package edu.mora.db.executor;

import edu.mora.db.catalog.Catalog;
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

class UpdateDeleteSqlTest {

    @TempDir
    Path dir;

    @Test
    void updateThenDeleteViaSql() throws Exception {
        /* boot */
        DiskManager disk = new DiskManager(dir.toString());
        BufferPool pool = new BufferPool(32, disk);
        WALManager wal = new WALManager(dir.toString());
        TransactionManager tm = new TransactionManager(wal, pool, disk);
        Catalog cat = new Catalog(dir.toString(), pool);
        SimpleExecutor exec = new SimpleExecutor(cat, tm);

        /* schema + seed data */
        exec.execute("CREATE TABLE t (id INT, name STRING)");
        exec.execute("INSERT INTO t VALUES (1, 'alice')");
        exec.execute("INSERT INTO t VALUES (2, 'bob')");

        /* UPDATE */
        exec.execute("UPDATE t SET name = 'ALICE' WHERE id = 1");

        /* DELETE */
        exec.execute("DELETE FROM t WHERE id = 2");

        /* verify */
        List<String> rows = new ArrayList<>();
        exec.execute("SELECT * FROM t", t -> rows.add(t.toString()));

        assertEquals(1, rows.size());
        assertTrue(rows.get(0).contains("ALICE"));
    }
}