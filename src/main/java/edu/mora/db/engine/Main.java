package edu.mora.db.engine;

import edu.mora.db.index.BPlusTreeIndex;
import edu.mora.db.storage.RecordId;

import java.util.List;

public class Main {
    public static void main(String[] args) {
        BPlusTreeIndex<Integer> tree = new BPlusTreeIndex<>(3);
        RecordId r = new RecordId(1, 0);
        tree.insert(10, r);

        List<RecordId> results = tree.search(10);
        System.out.println("Found " + results.size() + " entries:");
        for (RecordId rec : results) {
            System.out.printf("  page=%d offset=%d%n", rec.getPageId(), rec.getOffset());
        }
    }
}