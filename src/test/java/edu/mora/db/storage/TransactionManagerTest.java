package edu.mora.db.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that TransactionManager’s commit / rollback logic really makes it to disk and survives a restart.
 */
class TransactionManagerTest {

    @TempDir
    Path tempDir;

    private DiskManager disk;
    private BufferPool pool;
    private WALManager wal;
    private TransactionManager tm;

    @BeforeEach
    void init() throws IOException {
        disk = new DiskManager(tempDir.toString());
        pool = new BufferPool(4, disk);
        wal = new WALManager(tempDir.toString());
        tm = new TransactionManager(wal, pool, disk);
    }

    @Test
    void commitPersistsAfterRecovery() throws IOException {
        int pid = disk.allocatePage();
        Page p = pool.getPage(pid);

        long tx = tm.begin();
        byte[] before = p.getData().clone();
        p.getData()[0] = 55;
        byte[] after = p.getData().clone();
        tm.recordPageUpdate(tx, pid, before, after);
        tm.commit(tx);

        /* restart */
        DiskManager d2 = new DiskManager(tempDir.toString());
        BufferPool b2 = new BufferPool(4, d2);
        WALManager w2 = new WALManager(tempDir.toString());
        w2.recover(b2, d2);

        assertEquals(55, d2.readPage(pid)[0]);
    }

    @Test
    void rollbackRestoresBeforeImage() throws IOException {
        int pid = disk.allocatePage();
        Page p = pool.getPage(pid);

        long tx = tm.begin();
        byte[] before = p.getData().clone();
        p.getData()[0] = 77;
        byte[] after = p.getData().clone();
        tm.recordPageUpdate(tx, pid, before, after);
        tm.rollback(tx);

        assertEquals(0, disk.readPage(pid)[0], "byte should be back to original value");
    }
}