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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Executes parsed SQL.  Each INSERT / UPDATE / DELETE runs in its own single-statement transaction.
 */
public class SimpleExecutor {

    private static final Pattern HINT = Pattern.compile("^/\\*\\+\\s*(FAST|SAFE)\\s*\\*/", Pattern.CASE_INSENSITIVE);

    private final Catalog catalog;
    private final SQLParser parser = new SQLParser();
    private final TransactionManager tm;

    public SimpleExecutor(Catalog catalog, TransactionManager tm) {
        this.catalog = catalog;
        this.tm = tm;
    }

    /* --------------------------------------------------------------- */

    private static Hint extractHint(String s) {
        Matcher m = HINT.matcher(s.trim());
        if (m.find()) {
            boolean fast = m.group(1).equalsIgnoreCase("FAST");
            return new Hint(fast, s.substring(m.end()).trim());
        }
        return new Hint(false, s);
    }

    private static Tuple applyAssignments(Table tab, Tuple old, UpdateStatement upd) {
        Object[] vals = new Object[tab.getSchema().numColumns()];
        for (int i = 0; i < vals.length; i++) vals[i] = old.getField(i);

        upd.assignments().forEach((col, rawVal) -> {
            int idx = tab.getSchema().getColumnIndex(col);
            Schema.Type type = tab.getSchema().getColumnType(idx);

            /* ─── INT column: accept constants _or_ “col ± N” ───────── */
            if (type == Schema.Type.INT) {
                rawVal = rawVal.trim();
                if (rawVal.matches("\\d+")) {                 // simple constant
                    vals[idx] = Integer.parseInt(rawVal);
                } else {
                    // expect the form  "<col> [+|-] <number>"
                    java.util.regex.Matcher m =
                            java.util.regex.Pattern.compile("(\\w+)\\s*([+-])\\s*(\\d+)")
                                    .matcher(rawVal);
                    if (!m.matches())
                        throw new IllegalArgumentException("Unsupported expression: " + rawVal);

                    int base = (int) old.getField(idx);
                    int delta = Integer.parseInt(m.group(3));
                    vals[idx] = m.group(2).equals("+") ? base + delta : base - delta;
                }
            }
            /* ─── STRING column: only constants for now ─────────────── */
            else {
                vals[idx] = rawVal;
            }
        });
        return new Tuple(tab.getSchema(), vals);
    }

    /* =============================================================== */
    /*                  helpers & transaction wrapper                  */
    /* =============================================================== */

    private static Predicate<Tuple> predicate(Table tab, Optional<SelectStatement.Condition> opt) {
        if (opt.isEmpty()) return t -> true;
        int idx = tab.getSchema().getColumnIndex(opt.get().column());
        String v = opt.get().value();
        return t -> t.getField(idx).toString().equals(v);
    }

    public void execute(String rawSql) throws Exception {
        Hint h = extractHint(rawSql);           // peel off optional hint
        String sql = h.sql();                   // SQL without the hint
        Statement stmt = parser.parse(sql);

        switch (stmt) {
            case CreateTableStatement c -> execCreate(c);                       // DDL
            case InsertStatement i -> inTx(tx -> execInsert(tx, i), h.fast);
            case UpdateStatement u -> inTx(tx -> execUpdate(tx, u), h.fast);
            case DeleteStatement d -> inTx(tx -> execDelete(tx, d), h.fast);
            case SelectStatement s -> execSelect(s);                         // read-only
            default -> throw new IllegalArgumentException("Unknown SQL");
        }
    }

    /* SELECT with row-consumer */
    public void execute(String rawSql, java.util.function.Consumer<Tuple> sink) throws Exception {
        Hint h = extractHint(rawSql);
        if (h.fast) throw new IllegalArgumentException("FAST/SAFE hint meaningless for SELECT");

        Statement stmt = parser.parse(h.sql());
        if (!(stmt instanceof SelectStatement sel))
            throw new IllegalArgumentException("Only SELECT may use a row-consumer");

        execSelect(sel, sink);
    }

    /* =============================================================== */
    /*                       statement bodies                          */
    /* =============================================================== */

    private void inTx(TxBody body, boolean fast) throws Exception {
        long tx = tm.begin();
        try {
            body.run(tx);
            tm.commit(tx, fast);
        } catch (Exception e) {
            tm.rollback(tx);
            throw e;
        }
    }

    private void execCreate(CreateTableStatement c) throws IOException {
        Schema.Type[] types = c.columnTypes().toArray(Schema.Type[]::new);
        catalog.createTable(c.tableName(), new Schema(c.columnNames(), Arrays.asList(types)));
        System.out.println("Table " + c.tableName() + " created.");
    }

    private void execInsert(long tx, InsertStatement ins) throws IOException {
        Table table = catalog.getTable(ins.tableName());
        RecordId rid = table.insertTuple(tx, tm, new Tuple(table.getSchema(), ins.values().toArray()));
        System.out.println("Inserted at " + rid.getPageId() + "," + rid.getOffset());
    }

    private void execUpdate(long tx, UpdateStatement upd) throws IOException {
        Table tab = catalog.getTable(upd.tableName());
        Predicate<Tuple> pred = predicate(tab, upd.where());
        for (Table.Row row : tab.scanRows(pred)) {
            Tuple neu = applyAssignments(tab, row.tuple(), upd);
            tab.updateTuple(tx, tm, row.rid(), neu);
        }
        System.out.println("Updated rows.");
    }

    private void execDelete(long tx, DeleteStatement del) throws IOException {
        Table tab = catalog.getTable(del.tableName());
        Predicate<Tuple> pred = predicate(tab, del.where());
        for (Table.Row r : tab.scanRows(pred))
            tab.deleteTuple(tx, tm, r.rid());
        System.out.println("Deleted rows.");
    }

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

    private void execSelect(SelectStatement sel, java.util.function.Consumer<Tuple> sink) throws IOException {
        Table tab = catalog.getTable(sel.tableName());
        Predicate<Tuple> pred = predicate(tab, sel.where());
        for (Tuple t : tab.scan(pred)) sink.accept(t);
    }

    /* wrapper */
    @FunctionalInterface
    private interface TxBody {
        void run(long txId) throws Exception;
    }

    private record Hint(boolean fast, String sql) {
    }
}