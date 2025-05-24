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

    // LinkedHashMap in access-order to implement LRU
    private final Map<Integer, Page> cache = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, Page> eldest) {
            if (size() > BufferPool.this.capacity) {
                try {
                    evictPage(eldest.getKey(), eldest.getValue());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return true;
            }
            return false;
        }
    };

    public BufferPool(int capacity, DiskManager diskManager) {
        this.capacity = capacity;
        this.diskManager = diskManager;
    }

    /**
     * Fetches the page from cache or loads from disk if missing.
     */
    public synchronized Page getPage(int pageId) throws IOException {
        // hit?
        if (cache.containsKey(pageId)) {
            return cache.get(pageId);
        }
        // miss: load from disk
        byte[] data = diskManager.readPage(pageId);
        Page p = new Page(pageId);
        System.arraycopy(data, 0, p.getData(), 0, Page.PAGE_SIZE);
        cache.put(pageId, p);
        return p;
    }

    /**
     * Marks a page as dirty, so we know to flush it before eviction.
     */
    public synchronized void markDirty(int pageId, boolean dirty) {
        Page p = cache.get(pageId);
        if (p != null) p.markDirty(dirty);
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
     * Evict a single page: flush if dirty, then remove.
     */
    private void evictPage(int pageId, Page p) throws IOException {
        if (p.isDirty()) {
            diskManager.writePage(pageId, p.getData());
        }
        // after this return, LinkedHashMap will drop it
    }
}