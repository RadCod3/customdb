package edu.mora.db.engine;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.RecordId;
import edu.mora.db.table.Schema;
import edu.mora.db.table.Tuple;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
        String dbPath = ".";

        // Initialize DiskManager and BufferPool
        DiskManager disk = new DiskManager(dbPath);
        BufferPool buf = new BufferPool(100, disk);

        // Initialize or load catalog
        Catalog catalog = new Catalog(dbPath, buf);
        String tableName = "users";

        // On first run, create 'users' table
        if (!catalog.listTables().contains(tableName)) {
            System.out.println("Creating 'users' table...");
            Schema usersSchema = new Schema(
                    Arrays.asList("id", "name"),
                    Arrays.asList(Schema.Type.INT, Schema.Type.STRING)
            );
            catalog.createTable(tableName, usersSchema);
        }

        var users = catalog.getTable(tableName);

        // Load existing records
        List<Tuple> allUsers = users.scanAll();
        if (allUsers.isEmpty()) {
            // Insert sample users only once
            RecordId ridA = users.insertTuple(new Tuple(users.getSchema(), 1, "Alice"));
            RecordId ridB = users.insertTuple(new Tuple(users.getSchema(), 2, "Bob"));
            System.out.printf("Inserted sample users: %s, %s%n", ridA, ridB);
            // Flush buffered pages to disk
            buf.flushAll();
            allUsers = users.scanAll();
        }

        // Print all users
        System.out.println("Current users in database:");
        for (Tuple t : allUsers) {
            int id = (Integer) t.getField(0);
            String name = (String) t.getField(1);
            System.out.printf("- User %d = %s%n", id, name);
        }

        // Ensure all pages are persisted
        buf.flushAll();
        disk.close();
    }
}