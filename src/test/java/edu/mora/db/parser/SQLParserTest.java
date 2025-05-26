package edu.mora.db.parser;

import edu.mora.db.sql.InsertStatement;
import edu.mora.db.sql.SelectStatement;
import edu.mora.db.sql.Statement;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SQLParserTest {
    private final SQLParser parser = new SQLParser();

    @Test
    void testParseInsertStatement() {
        String sql = "INSERT INTO users VALUES (1, 'Alice')";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof InsertStatement, "Expected InsertStatement");
        InsertStatement ins = (InsertStatement) stmt;

        assertEquals("users", ins.tableName());
        List<String> values = ins.values();
        assertEquals(2, values.size());
        assertEquals("1", values.get(0));
        assertEquals("Alice", values.get(1));
    }

    @Test
    void testParseSelectWithoutWhere() {
        String sql = "SELECT * FROM products";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement, "Expected SelectStatement");
        SelectStatement sel = (SelectStatement) stmt;

        assertEquals("products", sel.tableName());
        assertFalse(sel.where().isPresent(), "Expected no WHERE clause");
    }

    @Test
    void testParseSelectWithWhere() {
        String sql = "SELECT * FROM orders WHERE status = 'shipped'";
        Statement stmt = parser.parse(sql);
        assertTrue(stmt instanceof SelectStatement, "Expected SelectStatement");
        SelectStatement sel = (SelectStatement) stmt;

        assertEquals("orders", sel.tableName());
        Optional<SelectStatement.Condition> where = sel.where();
        assertTrue(where.isPresent(), "Expected WHERE clause");
        SelectStatement.Condition cond = where.get();
        assertEquals("status", cond.column());
        assertEquals("shipped", cond.value());
    }

    @Test
    void testUnsupportedSqlThrows() {
        String sql = "UPDATE users SET name = 'Bob'";
        Exception ex = assertThrows(IllegalArgumentException.class, () -> parser.parse(sql));
        assertTrue(ex.getMessage().contains("Unsupported SQL"));
    }
}
