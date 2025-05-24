package edu.mora.db.index;

import edu.mora.db.storage.RecordId;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages multiple B+ tree indexes by name.
 */
public class IndexManager {
    private final Map<String, BPlusTreeIndex<?>> indexes = new HashMap<>();

    public <K extends Comparable<K>> void createIndex(String name, int order, Class<K> keyClass) {
        indexes.put(name, new BPlusTreeIndex<K>(order));
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<K>> void insert(String name, K key, RecordId rid) {
        BPlusTreeIndex<K> idx = (BPlusTreeIndex<K>) indexes.get(name);
        idx.insert(key, rid);
    }

    @SuppressWarnings("unchecked")
    public <K extends Comparable<K>> List<RecordId> search(String name, K key) {
        BPlusTreeIndex<K> idx = (BPlusTreeIndex<K>) indexes.get(name);
        return idx.search(key);
    }
}

