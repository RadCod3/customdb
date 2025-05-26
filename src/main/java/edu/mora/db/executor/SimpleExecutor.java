package edu.mora.db.executor;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.parser.SQLParser;
import edu.mora.db.sql.CreateTableStatement;
import edu.mora.db.sql.InsertStatement;
import edu.mora.db.sql.SelectStatement;
import edu.mora.db.sql.Statement;
import edu.mora.db.storage.RecordId;
import edu.mora.db.storage.TransactionManager;
import edu.mora.db.table.Schema;
import edu.mora.db.table.Table;
import edu.mora.db.table.Tuple;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

/**
 * Executes parsed SQL by delegating mutations to the TransactionManager so that every DML statement runs as an
 * independent transaction.
 */
public class SimpleExecutor {

    private final Catalog catalog;
    private final SQLParser parser = new SQLParser();
    private final TransactionManager tm;

    public SimpleExecutor(Catalog catalog, TransactionManager tm) {
        this.catalog = catalog;
        this.tm = tm;
    }

    public void execute(String sql) throws Exception {
        Statement stmt = parser.parse(sql);
        switch (stmt) {
            case CreateTableStatement c -> execCreate(c);                 // metadata only
            case InsertStatement i -> inTx(tx -> execInsert(tx, i)); // DML
            case SelectStatement s -> execSelect(s);                 // read-only
            default -> throw new IllegalArgumentException("Unknown SQL");
        }
    }

    public void execute(String sql,
                        java.util.function.Consumer<Tuple> rowConsumer) throws Exception {

        Statement stmt = parser.parse(sql);
        if (!(stmt instanceof SelectStatement sel)) {
            throw new IllegalArgumentException("Only SELECT may use a row-consumer");
        }
        execSelect(sel, rowConsumer);          // 👈 hand off
    }

    /* --------------------------------------------------------------- */

    /* --------------------------------------------------------------- */
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

    /* --------------------------------------------------------------- */
    private void execCreate(CreateTableStatement c) throws IOException {
        Schema.Type[] types = c.columnTypes().stream()
                .map(t -> t == Schema.Type.INT ? Schema.Type.INT : Schema.Type.STRING)
                .toArray(Schema.Type[]::new);

        catalog.createTable(c.tableName(), new Schema(c.columnNames(), List.of(types)));
        System.out.println("Table " + c.tableName() + " created.");
    }

    private void execInsert(long tx, InsertStatement ins) throws IOException {
        Table table = catalog.getTable(ins.tableName());
        RecordId rid = table.insertTuple(tx, tm, new Tuple(table.getSchema(), ins.values().toArray()));
        System.out.println("Inserted at " + rid.getPageId() + "," + rid.getOffset());
    }

    // default version used by the REPL
    private void execSelect(SelectStatement sel) throws IOException {
        execSelect(sel, System.out::println);
    }

    // worker that the tests (and other code) can call
    private void execSelect(SelectStatement sel,
                            java.util.function.Consumer<Tuple> sink) throws IOException {

        Table table = catalog.getTable(sel.tableName());
        Predicate<Tuple> pred = t -> true;

        if (sel.where().isPresent()) {
            var w = sel.where().get();
            int idx = table.getSchema().getColumnIndex(w.column());
            String v = w.value();
            pred = t -> t.getField(idx).toString().equals(v);
        }

        for (Tuple t : table.scan(pred)) sink.accept(t);
    }

    /* Functional helper – body that knows its txId. */
    @FunctionalInterface
    private interface TxBody {
        void run(long txId) throws Exception;
    }
}