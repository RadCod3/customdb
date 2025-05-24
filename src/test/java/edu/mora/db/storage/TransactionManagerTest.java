package edu.mora.db.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TransactionManagerTest {
    @TempDir
    Path tempDir;

    private DiskManager diskManager;
    private BufferPool bufferPool;
    private WALManager walManager;
    private TransactionManager txManager;

    @BeforeEach
    void setup() throws IOException {
        diskManager = new DiskManager(tempDir.toString());
        bufferPool = new BufferPool(5, diskManager);
        walManager = new WALManager(tempDir.toString());
        txManager = new TransactionManager(walManager, bufferPool, diskManager);
    }

    @Test
    void testCommitPersistsChangesThroughRecovery() throws IOException {
        int pageId = diskManager.allocatePage();
        // initial content should be zeros
        byte[] original = diskManager.readPage(pageId);
        assertEquals(0, original[0]);

        // begin and apply update
        long txId = txManager.begin();
        txManager.updatePage(txId, pageId, p -> p.getData()[0] = 55);
        txManager.commit(txId);

        // simulate restart
        DiskManager dm2 = new DiskManager(tempDir.toString());
        BufferPool bp2 = new BufferPool(5, dm2);
        WALManager wal2 = new WALManager(tempDir.toString());
        wal2.recover(bp2, dm2);

        byte[] afterRecovery = dm2.readPage(pageId);
        assertEquals(55, afterRecovery[0]);
    }

    @Test
    void testRollbackRevertsChanges() throws IOException {
        int pageId = diskManager.allocatePage();
        byte[] original = diskManager.readPage(pageId);
        assertEquals(0, original[0]);

        long txId = txManager.begin();
        txManager.updatePage(txId, pageId, p -> p.getData()[0] = 77);
        txManager.rollback(txId);

        byte[] afterRollback = diskManager.readPage(pageId);
        assertEquals(0, afterRollback[0]);
    }
}
