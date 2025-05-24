package edu.mora.db.index;

import edu.mora.db.storage.RecordId;

import java.util.ArrayList;
import java.util.List;

/**
 * Internal node: keys and children pointers.
 */
class BPlusInternalNode<K extends Comparable<K>> extends BPlusNode<K> {
    protected List<BPlusNode<K>> children;

    BPlusInternalNode(int order, List<K> keys, List<BPlusNode<K>> children) {
        super(order);
        this.keys = keys;
        this.children = children;
    }

    @Override
    boolean isLeaf() {
        return false;
    }

    @Override
    List<RecordId> search(K key) {
        // find child
        int idx = findChildIndex(key);
        return children.get(idx).search(key);
    }

    private int findChildIndex(K key) {
        int i = 0;
        while (i < keys.size() && key.compareTo(keys.get(i)) >= 0) i++;
        return i;
    }

    @Override
    void insert(K key, RecordId rid, BPlusTreeIndex<K> tree) {
        int idx = findChildIndex(key);
        BPlusNode<K> child = children.get(idx);
        child.insert(key, rid, tree);
        // after return, check if child split
        if (tree.splitEntry != null) {
            // incorporate splitEntry into this node
            K splitKey = tree.splitKey;
            BPlusNode<K> newNode = tree.splitEntry;
            tree.clearSplit();

            // insert in this internal node
            int insertPos = findChildIndex(splitKey);
            keys.add(insertPos, splitKey);
            children.set(insertPos, tree.splitEntry == null ? child : children.get(insertPos));
            children.add(insertPos + 1, newNode);

            if (keys.size() >= order) {
                // split this internal node
                int mid = keys.size() / 2;
                K upKey = keys.get(mid);

                List<K> rightKeys = new ArrayList<>(keys.subList(mid + 1, keys.size()));
                List<BPlusNode<K>> rightChildren = new ArrayList<>(children.subList(mid + 1, children.size()));

                // shrink this
                keys = new ArrayList<>(keys.subList(0, mid));
                children = new ArrayList<>(children.subList(0, mid + 1));

                BPlusInternalNode<K> sibling = new BPlusInternalNode<>(order, rightKeys, rightChildren);
                tree.splitKey = upKey;
                tree.splitEntry = sibling;
            }
        }
    }

    @Override
    K getFirstLeafKey() {
        return children.get(0).getFirstLeafKey();
    }
}
