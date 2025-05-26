package edu.mora.db.engine;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.executor.SimpleExecutor;
import edu.mora.db.storage.BufferPool;
import edu.mora.db.storage.DiskManager;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.storage.WALManager;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;

import java.io.IOException;

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

        /* ───── JLine Terminal with History ─────────────────────────────── */
        Terminal terminal = TerminalBuilder.builder().system(true).build();
        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .history(new DefaultHistory())
                .build();

        String line;
        while ((line = reader.readLine("MiniSQL> ")) != null) {
            if (line.equalsIgnoreCase("exit")) break;

            if (line.trim().equalsIgnoreCase("clear")) {
                clearScreen(terminal);
                continue;
            }

            if (line.trim().isEmpty()) continue;

            try {
                exec.execute(line);
            } catch (Exception e) {
                System.err.println("Error: " + e.getMessage());
            }
        }

        /* ───── clean shutdown ──────────────────────────────────────────── */
        pool.flushAll();
        wal.close();
        disk.close();
    }

    private static void clearScreen(Terminal terminal) {
        if ("dumb".equals(terminal.getType())) {
            // Dumb terminal fallback: print many newlines to simulate clearing
            for (int i = 0; i < 50; i++) {
                terminal.writer().println();
            }
            terminal.flush();
        } else {
            // Normal terminal: send ANSI clear screen codes
            terminal.writer().print("\033[H\033[2J");
            terminal.flush();
        }
    }
}
