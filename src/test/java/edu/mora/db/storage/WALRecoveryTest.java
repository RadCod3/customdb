package edu.mora.db.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Unit-tests the WAL itself (no TransactionManager or Table).
 */
class WALRecoveryTest {

    @TempDir
    Path tempDir;

    private DiskManager disk;
    private BufferPool pool;
    private WALManager wal;

    @BeforeEach
    void setUp() throws IOException {
        disk = new DiskManager(tempDir.toString());
        pool = new BufferPool(4, disk);
        wal = new WALManager(tempDir.toString());
    }

    @Test
    void committedUpdateIsRedone() throws IOException {
        int pid = disk.allocatePage();

        /* ---- TX 1: update first byte to 42 and COMMIT ---- */
        wal.logBegin(1);
        Page p = pool.getPage(pid);
        byte[] before = p.getData().clone();
        p.getData()[0] = 42;
        byte[] after = p.getData().clone();
        wal.logUpdate(1, pid, before, after);
        wal.logCommit(1);
        wal.flush();

        /* ---- simulate crash (discard in-memory state) ---- */
        disk = new DiskManager(tempDir.toString());
        pool = new BufferPool(4, disk);
        WALManager wal2 = new WALManager(tempDir.toString());
        wal2.recover(pool, disk);

        assertEquals(42, disk.readPage(pid)[0], "redo should apply committed update");
    }

    @Test
    void uncommittedUpdateIsUndone() throws IOException {
        int pid = disk.allocatePage();

        /* ---- TX 2: write byte 99 but do NOT commit ---- */
        wal.logBegin(2);
        Page p = pool.getPage(pid);
        byte[] before = p.getData().clone();
        p.getData()[0] = 99;
        byte[] after = p.getData().clone();
        wal.logUpdate(2, pid, before, after);
        wal.flush();

        /* ---- crash & recover ---- */
        disk = new DiskManager(tempDir.toString());
        pool = new BufferPool(4, disk);
        WALManager wal2 = new WALManager(tempDir.toString());
        wal2.recover(pool, disk);

        assertNotEquals(99, disk.readPage(pid)[0], "uncommitted update must NOT be redone");
    }
}