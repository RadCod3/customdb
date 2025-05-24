package edu.mora.db.index;

import edu.mora.db.storage.RecordId;

import java.util.ArrayList;
import java.util.List;

/**
 * Leaf node: keys and record pointers, with sibling link.
 */
class BPlusLeafNode<K extends Comparable<K>> extends BPlusNode<K> {
    protected List<RecordId> pointers;
    protected BPlusLeafNode<K> next;

    BPlusLeafNode(int order) {
        super(order);
        this.keys = new ArrayList<>();
        this.pointers = new ArrayList<>();
        this.next = null;
    }

    @Override
    boolean isLeaf() {
        return true;
    }

    @Override
    List<RecordId> search(K key) {
        List<RecordId> result = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).compareTo(key) == 0) {
                result.add(pointers.get(i));
            }
        }
        return result;
    }

    @Override
    void insert(K key, RecordId rid, BPlusTreeIndex<K> tree) {
        // find position
        int pos = 0;
        while (pos < keys.size() && key.compareTo(keys.get(pos)) >= 0) pos++;
        keys.add(pos, key);
        pointers.add(pos, rid);

        if (keys.size() >= order) {
            // split leaf
            int mid = keys.size() / 2;
            BPlusLeafNode<K> sibling = new BPlusLeafNode<>(order);
            sibling.keys.addAll(keys.subList(mid, keys.size()));
            sibling.pointers.addAll(pointers.subList(mid, pointers.size()));

            // shrink this
            keys = new ArrayList<>(keys.subList(0, mid));
            pointers = new ArrayList<>(pointers.subList(0, mid));

            sibling.next = this.next;
            this.next = sibling;

            tree.splitKey = sibling.keys.get(0);
            tree.splitEntry = sibling;
        }
    }

    @Override
    K getFirstLeafKey() {
        return keys.get(0);
    }
}

