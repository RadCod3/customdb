package edu.mora.db.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Very small transaction manager: each statement = one TX. All updates come in already applied; we just log and
 * remember before-images in case of rollback.
 */
public class TransactionManager {

    private final AtomicLong nextTxId = new AtomicLong(1);
    private final WALManager wal;
    private final BufferPool pool;
    private final DiskManager disk;

    private final Map<Long, List<UpdateRecord>> updates = new ConcurrentHashMap<>();

    public TransactionManager(WALManager wal, BufferPool pool, DiskManager disk) {
        this.wal = wal;
        this.pool = pool;
        this.disk = disk;
    }

    /* ───────────────────────────────────────────── */
    public long begin() throws IOException {
        long id = nextTxId.getAndIncrement();
        wal.logBegin(id);
        updates.put(id, new ArrayList<>());
        return id;
    }

    /**
     * Called by Table AFTER it has modified the page in memory.
     */
    public void recordPageUpdate(long txId, int pageId,
                                 byte[] before, byte[] after) throws IOException {
        wal.logUpdate(txId, pageId, before, after);
        wal.flush();                           // write-ahead rule
        pool.markDirty(pageId, true);          // page is already dirty
        updates.get(txId).add(new UpdateRecord(pageId, before));
    }

    public void commit(long txId) throws IOException {
        wal.logCommit(txId);
        wal.flush();
        pool.flushAll();
        updates.remove(txId);
    }

    public void rollback(long txId) throws IOException {
        wal.logAbort(txId);
        List<UpdateRecord> list = updates.get(txId);
        if (list != null) {
            for (int i = list.size() - 1; i >= 0; i--) {
                UpdateRecord r = list.get(i);
                // restore before-image both in buffer and on disk
                Page p = pool.getPage(r.pageId);
                System.arraycopy(r.before, 0, p.getData(), 0, Page.PAGE_SIZE);
                pool.markDirty(r.pageId, true);
                disk.writePage(r.pageId, r.before);
            }
            pool.flushAll();
            updates.remove(txId);
        }
    }

    /* ───────────────────────────────────────────── */
    private static class UpdateRecord {
        final int pageId;
        final byte[] before;

        UpdateRecord(int pid, byte[] img) {
            this.pageId = pid;
            this.before = img;
        }
    }
}