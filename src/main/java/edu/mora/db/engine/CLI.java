package edu.mora.db.engine;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.executor.SimpleExecutor;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.storage.WALManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * A tiny REPL that brings the whole engine up, including WAL recovery, and wraps every data-changing statement in its
 * own transaction.
 */
public class CLI {

    public static void main(String[] args) throws IOException {
        String dbPath = args.length > 0 ? args[0] : ".";

        /* ───── bootstrap the storage layer ─────────────────────────────── */
        DiskManager disk = new DiskManager(dbPath);
        BufferPool pool = new BufferPool(100, disk);
        WALManager wal = new WALManager(dbPath);
        wal.recover(pool, disk);                // REDO / UNDO
        TransactionManager txm = new TransactionManager(wal, pool, disk);

        /* ───── logical layer ───────────────────────────────────────────── */
        Catalog catalog = new Catalog(dbPath, pool);
        SimpleExecutor exec = new SimpleExecutor(catalog, txm);

        /* ───── REPL loop ───────────────────────────────────────────────── */
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        System.out.print("MiniSQL> ");
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.equalsIgnoreCase("exit")) break;
            if (line.trim().isEmpty()) {
                System.out.print("MiniSQL> ");
                continue;
            }

            try {
                exec.execute(line);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
            System.out.print("MiniSQL> ");
        }

        /* ───── clean shutdown ──────────────────────────────────────────── */
        pool.flushAll();
        wal.close();
        disk.close();
    }
}