package edu.mora.db.index;

import edu.mora.db.storage.RecordId;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory B+ tree index supporting insert and point search.
 */
public class BPlusTreeIndex<K extends Comparable<K>> {
    private final int order;
    BPlusNode<K> root;

    // temporary split info
    K splitKey;
    BPlusNode<K> splitEntry;

    public BPlusTreeIndex(int order) {
        this.order = order;
        // start with a single leaf
        this.root = new BPlusLeafNode<>(order);
    }

    public void insert(K key, RecordId rid) {
        splitKey = null;
        splitEntry = null;
        root.insert(key, rid, this);
        if (splitEntry != null) {
            // root was split
            List<K> newKeys = new ArrayList<>();
            newKeys.add(splitKey);
            List<BPlusNode<K>> children = new ArrayList<>();
            children.add(root);
            children.add(splitEntry);
            root = new BPlusInternalNode<>(order, newKeys, children);
            clearSplit();
        }
    }

    public List<RecordId> search(K key) {
        return root.search(key);
    }

    /**
     * Clears split state after handling a split.
     */
    public void clearSplit() {
        splitKey = null;
        splitEntry = null;
    }
}
