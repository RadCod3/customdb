package edu.mora.db.table;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.storage.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates basic record life-cycle operations on a single page.
 */
class TableBasicCrudTest {

    @TempDir
    Path dir;

    private BufferPool pool;
    private DiskManager disk;
    private TransactionManager tm;
    private Table table;

    @BeforeEach
    void setUp() throws IOException {
        disk = new DiskManager(dir.toString());
        pool = new BufferPool(8, disk);                 // plenty
        WALManager wal = new WALManager(dir.toString());
        tm = new TransactionManager(wal, pool, disk);
        Catalog cat = new Catalog(dir.toString(), pool);

        Schema s = new Schema(
                java.util.List.of("id", "name"),
                java.util.List.of(Schema.Type.INT, Schema.Type.STRING));
        cat.createTable("emp", s);
        table = cat.getTable("emp");
    }

    @Test
    void insertUpdateDeleteRoundTrip() throws Exception {
        /* ---------- INSERT ---------- */
        long tx1 = tm.begin();
        Tuple t1 = new Tuple(table.getSchema(), 1, "alice");
        RecordId rid = table.insertTuple(tx1, tm, t1);
        tm.commit(tx1);

        List<Tuple> rows = table.scanAll();
        assertEquals(1, rows.size());
        assertEquals("alice", rows.get(0).getField(1));

        /* ---------- UPDATE (same size) ---------- */
        long tx2 = tm.begin();
        Tuple t2 = new Tuple(table.getSchema(), 1, "ALICE");
        RecordId rid2 = table.updateTuple(tx2, tm, rid, t2);
        tm.commit(tx2);
        assertEquals(rid, rid2, "in-place update should keep same RID");

        assertEquals("ALICE", table.readTuple(rid2).getField(1));

        /* ---------- UPDATE (larger – relocate) --- */
        long tx3 = tm.begin();
        Tuple t3 = new Tuple(table.getSchema(), 1, "ALICIA-LONG-NAME");
        RecordId rid3 = table.updateTuple(tx3, tm, rid2, t3);
        tm.commit(tx3);
        assertNotEquals(rid2, rid3, "big update should relocate");

        assertEquals("ALICIA-LONG-NAME", table.readTuple(rid3).getField(1));

        /* ---------- DELETE ---------- */
        long tx4 = tm.begin();
        table.deleteTuple(tx4, tm, rid3);
        tm.commit(tx4);

        assertTrue(table.scanAll().isEmpty(), "table should be empty after delete");
    }
}