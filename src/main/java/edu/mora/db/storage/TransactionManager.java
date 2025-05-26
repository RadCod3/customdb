package edu.mora.db.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Transaction manager that now supports two commit flavours: – SAFE  (default) waits for WAL fsync + dirty-page flush.
 * – FAST  returns immediately; a background “hardener” thread takes care of durability eventually.
 */
public class TransactionManager {

    private final WALManager wal;
    private final BufferPool pool;
    private final DiskManager disk;

    private final Map<Long, List<UpdateRecord>> updates = new ConcurrentHashMap<>();
    /* ───── background hardener ──────────────── */
    private final ScheduledExecutorService hardener =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "wal-hardener");
                t.setDaemon(true);
                return t;
            });
    private long nextTxId = 1;

    public TransactionManager(WALManager wal, BufferPool pool, DiskManager disk) {
        this.wal = wal;
        this.pool = pool;
        this.disk = disk;

        // every 10 ms force WAL + dirty pages to disk
        hardener.scheduleAtFixedRate(() -> {
            try {
                wal.flush();
                pool.flushAll();
            } catch (IOException e) {
                // best-effort; print once per failure kind
                e.printStackTrace();
            }
        }, 10, 10, TimeUnit.MILLISECONDS);
    }

    /* ──────────────────────────────── TX API ── */
    public synchronized long begin() throws IOException {
        long id = nextTxId++;
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

    /* ---------- commit paths ---------- */
    public void commit(long txId) throws IOException {
        commit(txId, /*fast=*/false);
    }

    /**
     * @param fast true  →  FAST commit (return before fsync) false →  SAFE commit (fsync + page flush synchronous)
     */
    public void commit(long txId, boolean fast) throws IOException {
        wal.logCommit(txId);

        if (!fast) {                    // SAFE path
            wal.flush();
            pool.flushAll();
        }
        updates.remove(txId);           // forget before-images
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

    /* ---------- shutdown ---------- */
    public void close() throws IOException {
        hardener.shutdownNow();
        try {
            hardener.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
        }
        wal.flush();
        pool.flushAll();
        wal.close();
    }

    /* ───────── helper record ───────── */
    private record UpdateRecord(int pageId, byte[] before) {
    }
}