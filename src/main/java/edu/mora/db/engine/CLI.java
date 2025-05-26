package edu.mora.db.engine;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.executor.SimpleExecutor;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * A simple REPL for SQL commands.
 */
public class CLI {
    public static void main(String[] args) throws IOException {
        String dbPath = args.length > 0 ? args[0] : ".";
        var disk = new DiskManager(dbPath);
        var buf = new BufferPool(100, disk);
        var catalog = new Catalog(dbPath, buf);
        var exec = new SimpleExecutor(catalog);

        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("MiniSQL> ");
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.equalsIgnoreCase("exit")) break;
            if (line.trim().isEmpty()) continue;
            try {
                exec.execute(line);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
            System.out.print("MiniSQL> ");
        }
        disk.close();
    }
}
