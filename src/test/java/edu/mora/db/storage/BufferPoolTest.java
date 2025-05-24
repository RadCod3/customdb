package edu.mora.db.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BufferPoolTest {

    @TempDir
    Path tempDir;

    @Test
    public void testCacheAndFlush() throws IOException {
        // set up
        DiskManager disk = new DiskManager(tempDir.toString());
        BufferPool buf = new BufferPool(2, disk);

        // allocate two pages
        int p1 = disk.allocatePage();
        int p2 = disk.allocatePage();

        // modify p1
        Page page1 = buf.getPage(p1);
        byte[] data1 = page1.getData();
        data1[0] = 42;
        buf.markDirty(p1, true);

        // modify p2
        Page page2 = buf.getPage(p2);
        byte[] data2 = page2.getData();
        data2[0] = 84;
        buf.markDirty(p2, true);

        // flush and verify on disk
        buf.flushAll();
        byte[] diskData1 = disk.readPage(p1);
        byte[] diskData2 = disk.readPage(p2);
        assertEquals(42, diskData1[0]);
        assertEquals(84, diskData2[0]);
    }

    @Test
    public void testEvictionEvictsLeastRecentlyUsed() throws IOException {
        // capacity = 2
        DiskManager disk = new DiskManager(tempDir.toString());
        BufferPool buf = new BufferPool(2, disk);

        // allocate three pages
        int p1 = disk.allocatePage();
        int p2 = disk.allocatePage();
        int p3 = disk.allocatePage();

        // touch p1, p2 in order
        buf.getPage(p1);
        buf.getPage(p2);

        // now access p1 again so p2 is LRU
        buf.getPage(p1);

        // modify p2 so it's dirty
        Page page2 = buf.getPage(p2);
        page2.getData()[0] = 7;
        buf.markDirty(p2, true);

        // now load p3 into buffer → should evict p2? Actually p2 is LRU, so it should be evicted & flushed
        buf.getPage(p3);

        // p2 should have been flushed to disk
        byte[] diskData2 = disk.readPage(p2);
        assertEquals(7, diskData2[0]);

        // cleanup
        buf.flushAll();
    }
}