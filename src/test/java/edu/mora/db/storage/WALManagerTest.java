package edu.mora.db.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class WALManagerTest {

    @TempDir
    Path tempDir;

    private DiskManager disk;
    private BufferPool buf;
    private WALManager wal;

    @BeforeEach
    public void setup() throws IOException {
        disk = new DiskManager(tempDir.toString());
        buf = new BufferPool(5, disk);
        wal = new WALManager(tempDir.toString());
    }

    @Test
    public void testWALRecoverAppliesCommitted() throws IOException {
        // allocate a page and write initial data
        int pid = disk.allocatePage();
        byte[] original = disk.readPage(pid);

        // start transaction
        long txId = 1L;
        wal.logBegin(txId);

        // get before-image, modify via buffer
        Page p = buf.getPage(pid);
        byte[] before = Arrays.copyOf(p.getData(), Page.PAGE_SIZE);
        p.getData()[0] = 99;
        byte[] after = Arrays.copyOf(p.getData(), Page.PAGE_SIZE);

        // log the update and mark dirty
        wal.logUpdate(txId, pid, before, after);
        buf.markDirty(pid, true);

        // commit
        wal.logCommit(txId);

        // simulate a fresh restart: discard in-memory state
        disk = new DiskManager(tempDir.toString());
        buf = new BufferPool(5, disk);
        WALManager wal2 = new WALManager(tempDir.toString());

        // recover from the log
        wal2.recover(buf, disk);

        // page on disk should reflect the committed change
        byte[] diskData = disk.readPage(pid);
        assertEquals(99, diskData[0]);
    }

    @Test
    public void testWALDoesNotApplyUncommitted() throws IOException {
        int pid = disk.allocatePage();

        long txId = 2L;
        wal.logBegin(txId);

        Page p = buf.getPage(pid);
        byte[] before = Arrays.copyOf(p.getData(), Page.PAGE_SIZE);
        p.getData()[0] = 123;
        byte[] after = Arrays.copyOf(p.getData(), Page.PAGE_SIZE);

        wal.logUpdate(txId, pid, before, after);
        buf.markDirty(pid, true);

        // NO commit or abort

        // fresh restart
        disk = new DiskManager(tempDir.toString());
        buf = new BufferPool(5, disk);
        WALManager wal2 = new WALManager(tempDir.toString());

        // recover --> should not apply the uncommitted change
        wal2.recover(buf, disk);

        byte[] diskData = disk.readPage(pid);
        assertNotEquals(123, diskData[0], "Uncommitted update should not be redone");
    }
}