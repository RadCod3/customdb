package edu.mora.db.sql;

import edu.mora.db.table.Schema;

import java.util.List;

/**
 * Represents: CREATE TABLE tableName (col1 TYPE, col2 TYPE, ...);
 */
public record CreateTableStatement(String tableName, List<String> columnNames,
                                   List<Schema.Type> columnTypes) implements Statement {

}