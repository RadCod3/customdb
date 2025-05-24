package edu.mora.db.storage;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Simple LRU buffer pool.
 */
public class BufferPool {
    private final int capacity;
    private final DiskManager diskManager;
    private final LinkedHashMap<Integer, Page> cache;

    public BufferPool(int capacity, DiskManager diskManager) {
        this.capacity = capacity;
        this.diskManager = diskManager;
        // access-order for LRU tracking
        this.cache = new LinkedHashMap<>(16, 0.75f, true);
    }

    /**
     * Fetches the page from cache or loads from disk if missing. Automatically evicts if capacity exceeded.
     */
    public synchronized Page getPage(int pageId) throws IOException {
        // Hit?
        if (cache.containsKey(pageId)) {
            return cache.get(pageId);
        }
        // miss: load from disk
        byte[] data = diskManager.readPage(pageId);
        Page p = new Page(pageId);
        System.arraycopy(data, 0, p.getData(), 0, Page.PAGE_SIZE);
        cache.put(pageId, p);
        // Enforce capacity
        if (cache.size() > capacity) {
            evictDirtyOrLRU();
        }
        return p;
    }

    /**
     * Marks a page as dirty, so we know to flush it before eviction.
     */
    public synchronized void markDirty(int pageId, boolean dirty) {
        Page p = cache.get(pageId);
        if (p != null) {
            p.markDirty(dirty);
        }
    }

    /**
     * Flushes all dirty pages to disk.
     */
    public synchronized void flushAll() throws IOException {
        for (Page p : cache.values()) {
            if (p.isDirty()) {
                diskManager.writePage(p.getPageId(), p.getData());
                p.markDirty(false);
            }
        }
    }

    /**
     * Evicts the first dirty page (LRU dirty) or, if none, the LRU clean page.
     */
    private void evictDirtyOrLRU() throws IOException {
        // Find LRU dirty page
        for (Map.Entry<Integer, Page> e : cache.entrySet()) {
            if (e.getValue().isDirty()) {
                evictPage(e.getKey(), e.getValue());
                cache.remove(e.getKey());
                return;
            }
        }
        // Evict LRU clean page
        Map.Entry<Integer, Page> eldest = cache.entrySet().iterator().next();
        evictPage(eldest.getKey(), eldest.getValue());
        cache.remove(eldest.getKey());
    }

    /**
     * Flushes a single page if dirty, then removes it.
     */
    private void evictPage(int pageId, Page p) throws IOException {
        if (p.isDirty()) {
            diskManager.writePage(pageId, p.getData());
        }
    }
}
