package edu.mora.db;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.executor.SimpleExecutor;
import edu.mora.db.storage.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Full-stack test: SQL → Table → WAL → Disk; survives crashes.
 */
class EngineIntegrationTest {

    /** Tiny helper that spins up the whole engine stack. */
    private static record Engine(DiskManager disk,
                                 BufferPool pool,
                                 WALManager wal,
                                 TransactionManager tm,
                                 Catalog catalog,
                                 SimpleExecutor exec) implements AutoCloseable {

        static Engine start(String path) throws IOException {
            DiskManager disk = new DiskManager(path);
            BufferPool  pool = new BufferPool(32, disk);
            WALManager  wal  = new WALManager(path);
            wal.recover(pool, disk);
            TransactionManager tm = new TransactionManager(wal, pool, disk);
            Catalog catalog = new Catalog(path, pool);          // uses same pool
            SimpleExecutor exec = new SimpleExecutor(catalog, tm);
            return new Engine(disk, pool, wal, tm, catalog, exec);
        }
        public void close() throws IOException {
            pool.flushAll(); wal.close(); disk.close();
        }
    }

    @TempDir Path dir;

    @Test
    void insertIsVisibleAfterCrash() throws Exception {
        /* ── boot #1: create + insert ─────────────────────── */
        try (Engine e1 = Engine.start(dir.toString())) {
            e1.exec.execute("CREATE TABLE t (id INT, name STRING)");
            e1.exec.execute("INSERT INTO t VALUES (1, 'foo')");
            // do NOT flush buffer pool on purpose – mimic sudden crash
        }

        /* ── boot #2: recovery & select ───────────────────── */
        try (Engine e2 = Engine.start(dir.toString())) {
            List<String> out = new ArrayList<>();
            e2.exec.execute("SELECT * FROM t", tuple -> out.add(tuple.toString()));
            assertEquals(1, out.size());
            assertTrue(out.get(0).contains("foo"));
        }
    }

    @Test
    void abortedInsertDoesNotAppear() throws Exception {
        /* boot #1 */
        try (Engine e1 = Engine.start(dir.toString())) {
            e1.exec.execute("CREATE TABLE t (id INT, name STRING)");
            // manual transaction so that we can roll it back
            long tx = e1.tm.begin();
            var table = e1.catalog.getTable("t");
            table.insertTuple(tx, e1.tm, new edu.mora.db.table.Tuple(
                    table.getSchema(),
                    new Object[]{2, "bar"}));
            e1.tm.rollback(tx);
        }

        /* boot #2 */
        try (Engine e2 = Engine.start(dir.toString())) {
            List<String> rows = new ArrayList<>();
            e2.exec.execute("SELECT * FROM t", t -> rows.add(t.toString()));
            assertTrue(rows.isEmpty(), "rolled-back tuple must be gone");
        }
    }
}