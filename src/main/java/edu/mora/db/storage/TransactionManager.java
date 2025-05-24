package edu.mora.db.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Manages transactions: begin, update, commit, and rollback. Uses WALManager for logging durability and BufferPool for
 * page caching.
 */
public class TransactionManager {
    private final AtomicLong nextTxId = new AtomicLong(1);
    private final WALManager wal;
    private final BufferPool bufferPool;
    private final DiskManager diskManager;
    // Track per-transaction before-images for rollback
    private final Map<Long, List<UpdateRecord>> txUpdates = new ConcurrentHashMap<>();

    public TransactionManager(WALManager wal, BufferPool bufferPool, DiskManager diskManager) {
        this.wal = wal;
        this.bufferPool = bufferPool;
        this.diskManager = diskManager;
    }

    /**
     * Start a new transaction, returns its unique ID.
     */
    public long begin() throws IOException {
        long txId = nextTxId.getAndIncrement();
        wal.logBegin(txId);
        txUpdates.put(txId, new ArrayList<>());
        return txId;
    }

    /**
     * Update a page within the context of a transaction. - Loads page into buffer, captures before-image, - Applies
     * updater, captures after-image, - Logs the update, marks page dirty.
     */
    public void updatePage(long txId, int pageId, Consumer<Page> updater) throws IOException {
        // Load page
        Page p = bufferPool.getPage(pageId);
        // Capture before-image
        byte[] before = p.getData().clone();
        // Apply user-provided modification
        updater.accept(p);
        // Capture after-image
        byte[] after = p.getData().clone();

        // Log update and mark dirty
        wal.logUpdate(txId, pageId, before, after);
        bufferPool.markDirty(pageId, true);

        // Remember before-image to allow rollback
        txUpdates.get(txId).add(new UpdateRecord(pageId, before));
    }

    /**
     * Commit a transaction: log commit, flush all dirty pages to disk.
     */
    public void commit(long txId) throws IOException {
        wal.logCommit(txId);
        bufferPool.flushAll();
        txUpdates.remove(txId);
    }

    /**
     * Abort a transaction: log abort, undo all buffered changes, and flush.
     */
    public void rollback(long txId) throws IOException {
        wal.logAbort(txId);
        List<UpdateRecord> updates = txUpdates.get(txId);
        if (updates != null) {
            // Undo in reverse order
            for (int i = updates.size() - 1; i >= 0; i--) {
                UpdateRecord rec = updates.get(i);
                // Restore before-image in buffer
                Page p = bufferPool.getPage(rec.pageId);
                System.arraycopy(rec.before, 0, p.getData(), 0, Page.PAGE_SIZE);
                bufferPool.markDirty(rec.pageId, true);
                // Also write immediately to disk to maintain consistency
                diskManager.writePage(rec.pageId, rec.before);
            }
            bufferPool.flushAll();
            txUpdates.remove(txId);
        }
    }

    /**
     * Simple record of a page update's before-image.
     */
    private static class UpdateRecord {
        final int pageId;
        final byte[] before;

        UpdateRecord(int pageId, byte[] before) {
            this.pageId = pageId;
            this.before = before;
        }
    }
}
