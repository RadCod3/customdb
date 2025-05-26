package edu.mora.db.sql;

import java.util.Map;
import java.util.Optional;

public record UpdateStatement(
        String tableName,
        Map<String, String> assignments,
        Optional<SelectStatement.Condition> where) implements Statement {

}
