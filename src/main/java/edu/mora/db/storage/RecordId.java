package edu.mora.db.storage;

/**
 * A pointer to a record in the database: (pageId, offset)
 */
public class RecordId {
    private final int pageId;
    private final int offset;

    public RecordId(int pageId, int offset) {
        this.pageId = pageId;
        this.offset = offset;
    }

    public int getPageId() {
        return pageId;
    }

    public int getOffset() {
        return offset;
    }
}