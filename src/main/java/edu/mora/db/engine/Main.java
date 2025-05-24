package edu.mora.db.engine;

import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.Page;

import java.io.IOException;

public class Main {
    public static void main(String[] args) {
        try {
            DiskManager dm = new DiskManager(".");
            BufferPool bp = new BufferPool(2, dm); // capacity = 2 pages

            // Allocate pages
            int p1 = dm.allocatePage();
            int p2 = dm.allocatePage();
            int p3 = dm.allocatePage();
            int p4 = dm.allocatePage();
            System.out.println("Allocated pages: " + p1 + ", " + p2 + ", " + p3);
//            Add a press enter to continue
//            System.out.println("Press Enter to continue...");
//            System.in.read();


            // Modify page 1
            Page page1 = bp.getPage(p1);
            page1.getData()[0] = 100;
            bp.markDirty(p1, true);
            System.out.println("Modified and marked dirty page " + p1);
//            System.out.println("Press Enter to continue...");
//            System.in.read();

            // Modify page 2
            Page page2 = bp.getPage(p2);
            page2.getData()[0] = 3;
            bp.markDirty(p2, true);
            System.out.println("Modified and marked dirty page " + p2);
//            System.out.println("Press Enter to continue...");
//            System.in.read();

            // Accessing page 3 should evict the least recently used page (page 1)
            Page page3Loaded = bp.getPage(p3);
            System.out.println("Loaded page " + p3 + " into buffer (caused eviction)");
//            System.out.println("Press Enter to continue...");
//            System.in.read();

            // Flush remaining dirty pages
            bp.flushAll();
            System.out.println("Flushed all dirty pages to disk");

            dm.close();
            System.out.println("DiskManager closed successfully");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
