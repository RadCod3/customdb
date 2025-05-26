package edu.mora.db.catalog;

import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.table.Schema;
import edu.mora.db.table.Table;

import java.io.*;
import java.util.*;

/**
 * Catalog now also remembers which pages belong to each table, so that scans work correctly after a crash/restart.
 * <p>
 * catalog.meta layout ------------------- int    nextTableId int    tableCount REPEAT tableCount { UTF
 * tableName int         tableId int         colCount REPEAT colCount { UTF colName, int colTypeOrdinal } int
 * pageCount REPEAT pageCount { int pageId } }
 */
public class Catalog {

    private static final String CATALOG_FILE = "catalog.meta";

    private final File metaFile;
    private final DiskManager disk;
    private final BufferPool pool;

    private final Map<String, Schema> schemas = new LinkedHashMap<>();
    private final Map<String, Integer> tableIds = new LinkedHashMap<>();
    private final Map<String, List<Integer>> pages = new LinkedHashMap<>();
    private final Map<String, Table> tables = new LinkedHashMap<>();

    private int nextTableId = 1;

    /* ------------------------------------------------------------ */
    public Catalog(String dir, BufferPool pool) throws IOException {
        this.disk = new DiskManager(dir);
        this.pool = pool;
        this.metaFile = new File(dir, CATALOG_FILE);

        if (metaFile.exists()) load();
        else save();     // bootstrap
    }

    /* ------------------------------------------------------------ */
    public synchronized void createTable(String name, Schema schema) throws IOException {
        if (schemas.containsKey(name))
            throw new IllegalArgumentException("Table '" + name + "' already exists");

        int id = nextTableId++;
        schemas.put(name, schema);
        tableIds.put(name, id);
        pages.put(name, new ArrayList<>());             // empty list
        tables.put(name, new Table(name, schema, pool, disk, this)); // 👉 passes catalog

        save();                                         // durable DDL
    }

    /**
     * Called by Table when it allocates a new page.
     */
    public synchronized void registerPage(String tableName, int pageId) throws IOException {
        pages.get(tableName).add(pageId);
        save();                                         // flush meta incrementally
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public Set<String> listTables() {
        return Collections.unmodifiableSet(schemas.keySet());
    }

    /* ------------------------------------------------------------ */
    private void save() throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(metaFile))) {
            out.writeInt(nextTableId);
            out.writeInt(schemas.size());

            for (String name : schemas.keySet()) {
                Schema s = schemas.get(name);
                out.writeUTF(name);
                out.writeInt(tableIds.get(name));

                out.writeInt(s.numColumns());
                for (int i = 0; i < s.numColumns(); i++) {
                    out.writeUTF(s.getColumnName(i));
                    out.writeInt(s.getColumnType(i).ordinal());
                }

                List<Integer> plist = pages.get(name);
                out.writeInt(plist.size());
                for (int pid : plist) out.writeInt(pid);
            }
        }
    }

    private void load() throws IOException {
        try (DataInputStream in = new DataInputStream(new FileInputStream(metaFile))) {

            nextTableId = in.readInt();
            int tblCnt = in.readInt();

            for (int t = 0; t < tblCnt; t++) {
                String name = in.readUTF();
                int tblId = in.readInt();

                int colCnt = in.readInt();
                List<String> colNames = new ArrayList<>(colCnt);
                List<Schema.Type> colTypes = new ArrayList<>(colCnt);
                for (int c = 0; c < colCnt; c++) {
                    colNames.add(in.readUTF());
                    colTypes.add(Schema.Type.values()[in.readInt()]);
                }
                Schema schema = new Schema(colNames, colTypes);
                schemas.put(name, schema);
                tableIds.put(name, tblId);

                int pageCnt = in.readInt();
                List<Integer> plist = new ArrayList<>(pageCnt);
                for (int i = 0; i < pageCnt; i++) plist.add(in.readInt());
                pages.put(name, plist);

                tables.put(name, new Table(name, schema, pool, disk, this, plist));
            }
        }
    }
}