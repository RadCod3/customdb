package edu.mora.db.sql;

import java.util.Optional;

public record DeleteStatement(
        String tableName,
        Optional<SelectStatement.Condition> where) implements Statement {
}
