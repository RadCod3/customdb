package edu.mora.db.sql;

import java.util.Optional;

/**
 * Represents: SELECT * FROM tableName [WHERE column = value];
 */
public record SelectStatement(String tableName, Optional<Condition> where) implements Statement {

    public record Condition(String column, String value) {
    }
}

