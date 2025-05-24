package edu.mora.db.index;

import edu.mora.db.storage.RecordId;

import java.util.List;

/**
 * Abstract node in a B+ tree.
 */
abstract class BPlusNode<K extends Comparable<K>> {
    protected final int order;
    protected List<K> keys;

    BPlusNode(int order) {
        this.order = order;
    }

    abstract boolean isLeaf();
    abstract void insert(K key, RecordId rid, BPlusTreeIndex<K> tree);
    abstract List<RecordId> search(K key);
    abstract K getFirstLeafKey();
}
