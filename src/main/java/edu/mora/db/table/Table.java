package edu.mora.db.table;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.storage.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import static java.lang.Integer.BYTES;

/**
 * A heap-file table that logs every page update through the TransactionManager.
 */
public class Table {
    public static final int HEADER_SIZE = BYTES;                 // slot-count prefix
    private final String name;
    private final Schema schema;
    private final BufferPool bufPool;
    private final DiskManager disk;
    private final Catalog catalog;
    private final int capacityPerPage;
    private final List<Integer> pageIds;

    public Table(String name, Schema schema,
                 BufferPool pool, DiskManager disk,
                 Catalog catalog,
                 List<Integer> existingPages) {
        this.name = name;
        this.schema = schema;
        this.bufPool = pool;
        this.disk = disk;
        this.catalog = catalog;
        this.capacityPerPage = Page.PAGE_SIZE - HEADER_SIZE;
        this.pageIds = new ArrayList<>(existingPages);
    }

    public Table(String name, Schema schema,
                 BufferPool pool, DiskManager disk,
                 Catalog catalog) {
        this(name, schema, pool, disk, catalog, Collections.emptyList());
    }

    /* ─────────────────── INSERT ─────────────────────────────────── */
    public RecordId insertTuple(long tx, TransactionManager tm, Tuple t) throws IOException {
        byte[] rec = t.serialize();
        if (rec.length > capacityPerPage - BYTES)
            throw new IllegalArgumentException("Tuple too large");

        if (pageIds.isEmpty()) allocateFreshPage();
        int pid = findPageWithSpace(rec.length);
        Page p = bufPool.getPage(pid);
        byte[] before = p.getData().clone();

        int offset = writeRecordIntoPage(p, rec);
        byte[] after = p.getData().clone();

        tm.recordPageUpdate(tx, pid, before, after);
        return new RecordId(pid, offset);
    }

    /* ─────────────────── UPDATE ─────────────────────────────────── */
    public RecordId updateTuple(long tx, TransactionManager tm, RecordId rid, Tuple newT) throws IOException {
        byte[] rec = newT.serialize();
        Page p = bufPool.getPage(rid.getPageId());
        byte[] before = p.getData().clone();

        ByteBuffer data = ByteBuffer.wrap(p.getData());
        int oldLen = data.getInt(rid.getOffset());
        if (oldLen <= 0) throw new IllegalStateException("Cannot update deleted tuple");

        RecordId result;
        if (rec.length <= oldLen) {
            data.putInt(rid.getOffset(), rec.length);
            System.arraycopy(rec, 0, p.getData(), rid.getOffset() + BYTES, rec.length);
            result = rid;
        } else {
            data.putInt(rid.getOffset(), 0);                    // tombstone
            result = insertTuple(tx, tm, newT);                 // inserts logs itself
        }

        byte[] after = p.getData().clone();
        tm.recordPageUpdate(tx, rid.getPageId(), before, after);
        return result;
    }

    /* ─────────────────── DELETE ─────────────────────────────────── */
    public void deleteTuple(long tx, TransactionManager tm, RecordId rid) throws IOException {
        Page p = bufPool.getPage(rid.getPageId());
        byte[] before = p.getData().clone();

        ByteBuffer data = ByteBuffer.wrap(p.getData());
        data.putInt(rid.getOffset(), 0);

        byte[] after = p.getData().clone();
        tm.recordPageUpdate(tx, rid.getPageId(), before, after);
    }

    /* ─────────────────── READ / SCAN (unchanged) ────────────────── */
    public Tuple readTuple(RecordId rid) throws IOException {
        Page p = bufPool.getPage(rid.getPageId());
        ByteBuffer data = ByteBuffer.wrap(p.getData());
        int len = data.getInt(rid.getOffset());
        if (len <= 0) throw new IllegalStateException("Deleted tuple");

        byte[] rec = new byte[len];
        System.arraycopy(p.getData(), rid.getOffset() + BYTES, rec, 0, len);
        return Tuple.deserialize(schema, rec);
    }

    public List<Tuple> scan(Predicate<Tuple> pred) throws IOException {
        List<Tuple> out = new ArrayList<>();
        for (int pid : pageIds) {
            Page p = bufPool.getPage(pid);
            ByteBuffer data = ByteBuffer.wrap(p.getData());
            int slots = data.getInt(0);
            for (int i = 0; i < slots; i++) {
                int slotPos = Page.PAGE_SIZE - BYTES * (i + 1);
                int off = data.getInt(slotPos);
                int len = data.getInt(off);
                if (len <= 0) continue;
                byte[] rec = new byte[len];
                System.arraycopy(p.getData(), off + BYTES, rec, 0, len);
                Tuple t = Tuple.deserialize(schema, rec);
                if (pred.test(t)) out.add(t);
            }
        }
        return out;
    }

    public List<Tuple> scanAll() throws IOException {
        return scan(t -> true);
    }

    public List<Row> scanRows(Predicate<Tuple> pred) throws IOException {
        List<Row> out = new ArrayList<>();
        for (int pid : pageIds) {
            Page p = bufPool.getPage(pid);
            ByteBuffer data = ByteBuffer.wrap(p.getData());
            int slots = data.getInt(0);

            for (int i = 0; i < slots; i++) {
                int slotPos = Page.PAGE_SIZE - BYTES * (i + 1);
                int off = data.getInt(slotPos);
                int len = data.getInt(off);
                if (len <= 0) continue;                 // tombstone

                byte[] rec = new byte[len];
                System.arraycopy(p.getData(), off + BYTES, rec, 0, len);
                Tuple tup = Tuple.deserialize(schema, rec);

                if (pred.test(tup))
                    out.add(new Row(new RecordId(pid, off), tup));
            }
        }
        return out;
    }


    public Schema getSchema() {
        return schema;
    }

    /**
     * Allocates a brand-new page, records ownership in catalog.
     */
    private void allocateFreshPage() throws IOException {
        int pid = disk.allocatePage();
        pageIds.add(pid);

        /* the page is all-zero (slotCount = 0) already – nothing else to do */
        catalog.registerPage(name, pid);               // persist
    }

    /* ─────────────────── helpers ─────────────────────────────────── */
    private int findPageWithSpace(int recLen) throws IOException {
        for (int pid : pageIds) {
            Page p = bufPool.getPage(pid);
            ByteBuffer data = ByteBuffer.wrap(p.getData());
            int numSlots = data.getInt(0);
            int used = HEADER_SIZE;
            for (int j = 0; j < numSlots; j++) {
                int slotPos = Page.PAGE_SIZE - BYTES * (j + 1);
                used += BYTES + data.getInt(data.getInt(slotPos));
            }
            int slotDirStart = Page.PAGE_SIZE - BYTES * numSlots;
            int free = slotDirStart - used;
            if (free >= recLen + BYTES) return pid;
        }
        /* all full: allocate another and try again */
        allocateFreshPage();
        return pageIds.getLast();
    }

    /**
     * Writes the record into the page and returns its offset.
     */
    private int writeRecordIntoPage(Page p, byte[] rec) {
        ByteBuffer data = ByteBuffer.wrap(p.getData());
        int slots = data.getInt(0);
        int offset = HEADER_SIZE;
        for (int i = 0; i < slots; i++) {
            int slotPos = Page.PAGE_SIZE - BYTES * (i + 1);
            int off = data.getInt(slotPos);
            offset += BYTES + data.getInt(off);
        }
        /* record (len + body) */
        data.putInt(offset, rec.length);
        System.arraycopy(rec, 0, p.getData(), offset + BYTES, rec.length);
        /* slot entry */
        int slotPos = Page.PAGE_SIZE - BYTES * (slots + 1);
        data.putInt(slotPos, offset);
        data.putInt(0, slots + 1);
        return offset;
    }

    public static record Row(RecordId rid, Tuple tuple) {
    }
}