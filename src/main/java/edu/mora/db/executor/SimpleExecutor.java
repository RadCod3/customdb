package edu.mora.db.executor;

import edu.mora.db.catalog.Catalog;
import edu.mora.db.parser.SQLParser;
import edu.mora.db.sql.InsertStatement;
import edu.mora.db.sql.SelectStatement;
import edu.mora.db.sql.Statement;
import edu.mora.db.storage.RecordId;
import edu.mora.db.table.Tuple;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

/**
 * Executes parsed SQL against the catalog and tables.
 */
public class SimpleExecutor {
    private final Catalog catalog;
    private final SQLParser parser;

    public SimpleExecutor(Catalog catalog) {
        this.catalog = catalog;
        this.parser = new SQLParser();
    }

    public void execute(String sql) throws IOException {
        Statement stmt = parser.parse(sql);
        if (stmt instanceof InsertStatement) {
            execInsert((InsertStatement) stmt);
        } else if (stmt instanceof SelectStatement) {
            execSelect((SelectStatement) stmt);
        } else {
            throw new IllegalArgumentException("Unknown statement");
        }
    }

    private void execInsert(InsertStatement ins) throws IOException {
        var table = catalog.getTable(ins.tableName());
        List<String> vals = ins.values();
        Object[] fields = vals.toArray();
        RecordId rid = table.insertTuple(new Tuple(table.getSchema(), fields));
        System.out.println("Inserted at " + rid.getPageId() + "," + rid.getOffset());
    }

    private void execSelect(SelectStatement sel) throws IOException {
        var table = catalog.getTable(sel.tableName());
        Predicate<Tuple> pred = tuple -> true;
        if (sel.where().isPresent()) {
            var c = sel.where().get();
            int colIdx = table.getSchema().getColumnIndex(c.column());
            String val = c.value().replace("'", "");
            pred = tuple -> tuple.getField(colIdx).toString().equals(val);
        }
        List<Tuple> rows = table.scan(pred);
        for (Tuple t : rows) {
            System.out.println(t);
        }
    }
}
