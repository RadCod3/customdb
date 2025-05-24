package edu.mora.db.table;

import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.Page;
import edu.mora.db.storage.RecordId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static java.lang.Integer.BYTES;

/**
 * A heap-file table: fixed-length records with a slot directory at the end of each page. Supports insert, read, delete,
 * update, and scan operations.
 */
public class Table {
    public static final int HEADER_SIZE = BYTES; // slot count prefix size
    private final Schema schema;
    private final BufferPool bufPool;
    private final DiskManager disk;
    private final int capacityPerPage;

    public Table(Schema schema, BufferPool bufPool, DiskManager disk) {
        this.schema = schema;
        this.bufPool = bufPool;
        this.disk = disk;
        this.capacityPerPage = Page.PAGE_SIZE - HEADER_SIZE;
    }

    public RecordId insertTuple(Tuple t) throws IOException {
        byte[] rec = t.serialize();
        if (rec.length > capacityPerPage - BYTES)
            throw new IllegalArgumentException("Tuple too large");

        int pid = findPageWithSpace(rec.length);
        Page p = bufPool.getPage(pid);
        ByteBuffer data = ByteBuffer.wrap(p.getData());

        int numSlots = data.getInt(0);
        int recOffset = HEADER_SIZE;
        for (int i = 0; i < numSlots; i++) {
            int slotPos = Page.PAGE_SIZE - BYTES * (i + 1);
            int off = data.getInt(slotPos);
            int len = data.getInt(off);
            recOffset += BYTES + len;
        }

        // write length prefix + record bytes
        data.putInt(recOffset, rec.length);
        System.arraycopy(rec, 0, p.getData(), recOffset + BYTES, rec.length);

        // add slot
        int newSlotPos = Page.PAGE_SIZE - BYTES * (numSlots + 1);
        data.putInt(newSlotPos, recOffset);
        data.putInt(0, numSlots + 1);

        bufPool.markDirty(pid, true);
        return new RecordId(pid, recOffset);
    }

    public Tuple readTuple(RecordId rid) throws IOException {
        Page p = bufPool.getPage(rid.getPageId());
        ByteBuffer data = ByteBuffer.wrap(p.getData());

        int offset = rid.getOffset();
        int len = data.getInt(offset);
        if (len <= 0) throw new IllegalStateException("Cannot read deleted tuple");

        byte[] rec = new byte[len];
        System.arraycopy(p.getData(), offset + BYTES, rec, 0, len);
        return Tuple.deserialize(schema, rec);
    }

    // private helpers
    private int findPageWithSpace(int recLen) throws IOException {
        int total = disk.getNumPages();
        for (int i = 0; i < total; i++) {
            Page p = bufPool.getPage(i);
            ByteBuffer data = ByteBuffer.wrap(p.getData());
            int numSlots = data.getInt(0);
            int used = HEADER_SIZE;
            for (int j = 0; j < numSlots; j++) {
                int slotPos = Page.PAGE_SIZE - BYTES * (j + 1);
                int off = data.getInt(slotPos);
                int len = data.getInt(off);
                used += BYTES + len;
            }
            int slotDirStart = Page.PAGE_SIZE - BYTES * numSlots;
            int free = slotDirStart - used;
            if (free >= recLen + BYTES) {
                return i;
            }
        }
        return disk.allocatePage();
    }

    /**
     * Marks the tuple at rid as deleted (tombstone) by zeroing its length prefix.
     */
    public void deleteTuple(RecordId rid) throws IOException {
        Page p = bufPool.getPage(rid.getPageId());
        ByteBuffer data = ByteBuffer.wrap(p.getData());
        data.putInt(rid.getOffset(), 0);
        bufPool.markDirty(rid.getPageId(), true);
    }

    /**
     * Updates the tuple at rid. If new record fits in old space, overwrite in place;
     * otherwise tombstone the old and insert the new record elsewhere.
     * @return the RecordId of the updated tuple (old or new slot).
     */
    public RecordId updateTuple(RecordId rid, Tuple newT) throws IOException {
        byte[] rec = newT.serialize();
        Page p = bufPool.getPage(rid.getPageId());
        ByteBuffer data = ByteBuffer.wrap(p.getData());
        int offset = rid.getOffset();

        int oldLen = data.getInt(offset);
        if (oldLen <= 0) throw new IllegalStateException("Cannot update deleted tuple");

        if (rec.length <= oldLen) {
            data.putInt(offset, rec.length);
            System.arraycopy(rec, 0, p.getData(), offset + BYTES, rec.length);
            bufPool.markDirty(rid.getPageId(), true);
            return rid;
        } else {
            data.putInt(offset, 0);
            bufPool.markDirty(rid.getPageId(), true);
            return insertTuple(newT);
        }
    }

    /**
     * Scans all tuples in the table, returning those matching the predicate. Deleted tuples (tombstones) are skipped.
     */
    public List<Tuple> scan(Predicate<Tuple> pred) throws IOException {
        List<Tuple> results = new ArrayList<>();
        int numPages = disk.getNumPages();
        for (int pid = 0; pid < numPages; pid++) {
            Page p = bufPool.getPage(pid);
            ByteBuffer data = ByteBuffer.wrap(p.getData());
            int numSlots = data.getInt(0);
            for (int i = 0; i < numSlots; i++) {
                int slotPos = Page.PAGE_SIZE - BYTES * (i + 1);
                int offset = data.getInt(slotPos);
                int len = data.getInt(offset);
                if (len <= 0) continue;
                byte[] rec = new byte[len];
                System.arraycopy(p.getData(), offset + BYTES, rec, 0, len);
                Tuple t = Tuple.deserialize(schema, rec);
                if (pred.test(t)) results.add(t);
            }
        }
        return results;
    }

    /**
     * Convenience: return all tuples.
     */
    public List<Tuple> scanAll() throws IOException {
        return scan(t -> true);
    }

    /**
     * Expose schema for querying.
     */
    public Schema getSchema() {
        return schema;
    }

    public BufferPool getBufferPool() {
        return bufPool;
    }

    public DiskManager getDiskManager() {
        return disk;
    }


    private int slotPayloadSize(Page p, int numSlots) {
        ByteBuffer data = ByteBuffer.wrap(p.getData());
        int sum = 0;
        for (int i = 0; i < numSlots; i++) {
            int off = data.getInt(Page.PAGE_SIZE - BYTES * (i + 1));
            int len = data.getInt(off);
            sum += len;
        }
        return sum;
    }
}