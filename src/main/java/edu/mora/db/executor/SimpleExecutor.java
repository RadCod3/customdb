package edu.mora.db.executor;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.parser.SQLParser;
import edu.mora.db.sql.*;
import edu.mora.db.storage.RecordId;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.table.Schema;
import edu.mora.db.table.Table;
import edu.mora.db.table.Tuple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Executes parsed SQL.  Each INSERT / UPDATE / DELETE runs in its own single-statement transaction.
 */
public class SimpleExecutor {

    private final Catalog catalog;
    private final SQLParser parser = new SQLParser();
    private final TransactionManager tm;

    public SimpleExecutor(Catalog catalog, TransactionManager tm) {
        this.catalog = catalog;
        this.tm = tm;
    }

    /* ───────────────────────── helpers ──────────────────────────────── */
    private static Predicate<Tuple> predicate(Table tab,
                                              Optional<SelectStatement.Condition> opt) {
        if (opt.isEmpty()) return t -> true;

        SelectStatement.Condition w = opt.get();
        int idx = tab.getSchema().getColumnIndex(w.column());
        String v = w.value();
        return t -> t.getField(idx).toString().equals(v);
    }

    /* ───────────────────────── entry points ─────────────────────────── */
    public void execute(String sql) throws Exception {
        Statement stmt = parser.parse(sql);

        switch (stmt) {
            case CreateTableStatement c -> execCreate(c);                         // DDL
            case InsertStatement i -> inTx(tx -> execInsert(tx, i));         // DML
            case UpdateStatement u -> inTx(tx -> execUpdate(tx, u));
            case DeleteStatement d -> inTx(tx -> execDelete(tx, d));
            case SelectStatement s -> execSelect(s);                         // read-only
            default -> throw new IllegalArgumentException("Unknown SQL");
        }
    }

    public void execute(String sql,
                        java.util.function.Consumer<Tuple> rowConsumer) throws Exception {
        Statement stmt = parser.parse(sql);
        if (!(stmt instanceof SelectStatement sel)) {
            throw new IllegalArgumentException("Only SELECT may use a row-consumer");
        }
        execSelect(sel, rowConsumer);
    }

    /* ───────────────────── helper: 1-statement TX ────────────────────── */
    private void inTx(TxBody body) throws Exception {
        long tx = tm.begin();
        try {
            body.run(tx);
            tm.commit(tx);
        } catch (Exception e) {
            tm.rollback(tx);
            throw e;
        }
    }

    /* ──────────────────────────── DDL ────────────────────────────────── */
    private void execCreate(CreateTableStatement c) throws IOException {
        Schema.Type[] types = c.columnTypes().toArray(Schema.Type[]::new);
        catalog.createTable(c.tableName(),
                            new Schema(c.columnNames(), Arrays.asList(types)));
        System.out.println("Table " + c.tableName() + " created.");
    }

    /* ─────────────────────────── INSERT ─────────────────────────────── */
    private void execInsert(long tx, InsertStatement ins) throws IOException {
        Table table = catalog.getTable(ins.tableName());
        RecordId rid = table.insertTuple(tx, tm, new Tuple(table.getSchema(), ins.values().toArray()));
        System.out.println("Inserted at " + rid.getPageId() + "," + rid.getOffset());
    }

    /* ─────────────────────────── UPDATE ─────────────────────────────── */
    private void execUpdate(long tx, UpdateStatement upd) throws IOException {
        Table tab = catalog.getTable(upd.tableName());

        Predicate<Tuple> pred = predicate(tab, upd.where());
        for (Table.Row row : tab.scanRows(pred)) {
            Tuple old = row.tuple();
            Object[] vals = new Object[tab.getSchema().numColumns()];
            for (int i = 0; i < vals.length; i++) vals[i] = old.getField(i);

            // apply SET assignments
            for (var e : upd.assignments().entrySet()) {
                int idx = tab.getSchema().getColumnIndex(e.getKey());
                if (tab.getSchema().getColumnType(idx) == Schema.Type.INT)
                    vals[idx] = Integer.parseInt(e.getValue());
                else
                    vals[idx] = e.getValue();
            }
            Tuple neu = new Tuple(tab.getSchema(), vals);
            tab.updateTuple(tx, tm, row.rid(), neu);
        }
        System.out.println("Updated rows.");
    }

    /* ─────────────────────────── DELETE ─────────────────────────────── */
    private void execDelete(long tx, DeleteStatement del) throws IOException {
        Table tab = catalog.getTable(del.tableName());

        Predicate<Tuple> pred = predicate(tab, del.where());
        for (Table.Row row : tab.scanRows(pred))
            tab.deleteTuple(tx, tm, row.rid());

        System.out.println("Deleted rows.");
    }

    /* ─────────────────────────── SELECT ─────────────────────────────── */
//    private void execSelect(SelectStatement sel) throws IOException {
//        execSelect(sel, System.out::println);
//    }

    private void execSelect(SelectStatement sel) throws IOException {
        Table tab = catalog.getTable(sel.tableName());
        Predicate<Tuple> pred = predicate(tab, sel.where());

        // Get column names from the table schema
        String[] columnNames = tab.getSchema().getColumnNames().toArray(new String[0]);

        // Collect tuples to format output nicely
        List<Tuple> results = new ArrayList<>();
        for (Tuple t : tab.scan(pred)) {
            results.add(t);
        }

        printTable(columnNames, results);
    }

    private void printTable(String[] columns, List<Tuple> rows) {
        // Calculate max width per column for formatting
        int[] colWidths = new int[columns.length];
        for (int i = 0; i < columns.length; i++) {
            colWidths[i] = columns[i].length();
        }

        // Update max width based on content
        for (Tuple row : rows) {
            for (int i = 0; i < columns.length; i++) {
                String val = row.getValue(i).toString();  // assuming getValue(int colIndex)
                colWidths[i] = Math.max(colWidths[i], val.length());
            }
        }

        // Print header
        StringBuilder header = new StringBuilder("|");
        StringBuilder separator = new StringBuilder("+");
        for (int i = 0; i < columns.length; i++) {
            header.append(String.format(" %-" + colWidths[i] + "s |", columns[i]));
            separator.append("-".repeat(colWidths[i] + 2)).append("+");
        }
        System.out.println(separator);
        System.out.println(header);
        System.out.println(separator);

        // Print rows
        for (Tuple row : rows) {
            StringBuilder rowLine = new StringBuilder("|");
            for (int i = 0; i < columns.length; i++) {
                String val = row.getValue(i).toString();
                rowLine.append(String.format(" %-" + colWidths[i] + "s |", val));
            }
            System.out.println(rowLine);
        }
        System.out.println(separator);
    }

    private void execSelect(SelectStatement sel,
                            java.util.function.Consumer<Tuple> sink) throws IOException {
        Table tab = catalog.getTable(sel.tableName());
        Predicate<Tuple> pred = predicate(tab, sel.where());
        for (Tuple t : tab.scan(pred)) sink.accept(t);
    }

    @FunctionalInterface
    private interface TxBody {
        void run(long txId) throws Exception;
    }
}