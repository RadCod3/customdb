package edu.mora.db.sql;

import java.util.List;

/**
 * Represents: INSERT INTO tableName VALUES (val1, val2, ...);
 */
public record InsertStatement(String tableName, List<String> values) implements Statement {
}
