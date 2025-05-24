package edu.mora.db.storage;

import java.io.IOException;
import java.io.RandomAccessFile;

public class DiskManager {
    private static final String DB_FILE = "database.db";
    private final RandomAccessFile file;
    private int nextPageId = 0;

    public DiskManager(String path) throws IOException {
        this.file = new RandomAccessFile(path + "/" + DB_FILE, "rw");
        this.nextPageId = (int) (file.length() / Page.PAGE_SIZE);
    }

    public int allocatePage() throws IOException {
        int pageId = nextPageId++;
        writePage(pageId, new byte[Page.PAGE_SIZE]);
        return pageId;
    }

    public void writePage(int pageId, byte[] data) throws IOException {
        file.seek((long) pageId * Page.PAGE_SIZE);
        file.write(data);
    }

    public byte[] readPage(int pageId) throws IOException {
        byte[] data = new byte[Page.PAGE_SIZE];
        file.seek((long) pageId * Page.PAGE_SIZE);
        file.readFully(data);
        return data;
    }

    public void close() throws IOException {
        file.close();
    }
}
