package edu.mora.db.catalog;

import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.table.Schema;
import edu.mora.db.table.Table;

import java.io.*;
import java.util.*;

/**
 * A simple catalog that persists table schemas on disk and allows lookup by name. Catalog file format (catalog.meta):
 * int    numTables for each table: UTF   tableName int   numColumns for each column: UTF name int typeOrdinal
 */
public class Catalog {
    private static final String CATALOG_FILE = "catalog.meta";

    private final File metaFile;
    private final DiskManager diskManager;
    private final BufferPool bufferPool;
    private final Map<String, Schema> schemas = new LinkedHashMap<>();
    private final Map<String, Table> tables = new LinkedHashMap<>();

    /**
     * Initialize catalog in the given directory. Loads existing metadata if present.
     */
    public Catalog(String dbPath, BufferPool bufferPool) throws IOException {
        this.bufferPool = bufferPool;
        this.diskManager = new DiskManager(dbPath);
        this.metaFile = new File(dbPath, CATALOG_FILE);
        if (metaFile.exists()) {
            load();
        } else {
            save();
        }
    }

    /**
     * Create a new table with the given name and schema. Persists metadata immediately.
     */
    public void createTable(String tableName, Schema schema) throws IOException {
        if (schemas.containsKey(tableName)) {
            throw new IllegalArgumentException("Table '" + tableName + "' already exists");
        }
        schemas.put(tableName, schema);
        tables.put(tableName, new Table(schema, bufferPool, diskManager));
        save();
    }

    /**
     * Get the Table instance for the given name, or null if not found.
     */
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    /**
     * List all table names in this catalog.
     */
    public Set<String> listTables() {
        return Collections.unmodifiableSet(schemas.keySet());
    }

    private void save() throws IOException {
        try (DataOutputStream out = new DataOutputStream(new FileOutputStream(metaFile))) {
            out.writeInt(schemas.size());
            for (Map.Entry<String, Schema> e : schemas.entrySet()) {
                out.writeUTF(e.getKey());
                Schema s = e.getValue();
                int cols = s.numColumns();
                out.writeInt(cols);
                for (int i = 0; i < cols; i++) {
                    out.writeUTF(s.getColumnName(i));
                    out.writeInt(s.getColumnType(i).ordinal());
                }
            }
        }
    }

    private void load() throws IOException {
        try (DataInputStream in = new DataInputStream(new FileInputStream(metaFile))) {
            int tablesCount = in.readInt();
            for (int t = 0; t < tablesCount; t++) {
                String name = in.readUTF();
                int cols = in.readInt();
                List<String> colNames = new ArrayList<>(cols);
                List<Schema.Type> colTypes = new ArrayList<>(cols);
                for (int i = 0; i < cols; i++) {
                    colNames.add(in.readUTF());
                    int ord = in.readInt();
                    colTypes.add(Schema.Type.values()[ord]);
                }
                Schema schema = new Schema(colNames, colTypes);
                schemas.put(name, schema);
                tables.put(name, new Table(schema, bufferPool, diskManager));
            }
        }
    }
}
