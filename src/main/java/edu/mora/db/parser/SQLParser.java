package edu.mora.db.parser;

import edu.mora.db.sql.InsertStatement;
import edu.mora.db.sql.SelectStatement;
import edu.mora.db.sql.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Very simple SQL parser for INSERT and SELECT.
 */
public class SQLParser {
    public Statement parse(String sql) {
        sql = sql.trim();
        String upperSql = sql.toUpperCase();
        if (upperSql.startsWith("INSERT")) return parseInsert(sql);
        if (upperSql.startsWith("SELECT")) return parseSelect(sql);
        throw new IllegalArgumentException("Unsupported SQL: " + sql);
    }

    private InsertStatement parseInsert(String sql) {
        // INSERT INTO tableName VALUES (v1, v2, ...)
        String upperSql = sql.toUpperCase();
        int valuesIdx = upperSql.indexOf("VALUES");
        String head = sql.substring(0, valuesIdx).trim();           // INSERT INTO tableName
        String tail = sql.substring(valuesIdx + "VALUES".length()).trim(); // (v1, v2, ...)

        String[] headTokens = head.split("\s+");
        String tableName = headTokens[2];

        String valuesList = tail.substring(tail.indexOf('(') + 1, tail.lastIndexOf(')'));
        List<String> vals = new ArrayList<>();
        for (String v : valuesList.split(",")) {
            String trimmed = v.trim();
            // remove surrounding single quotes if present
            if (trimmed.startsWith("'") && trimmed.endsWith("'")) {
                trimmed = trimmed.substring(1, trimmed.length() - 1);
            }
            vals.add(trimmed);
        }
        return new InsertStatement(tableName, vals);
    }

    private SelectStatement parseSelect(String sql) {
        // SELECT * FROM tableName [WHERE col = val]
        String upperSql = sql.trim().toUpperCase();
        // extract part after FROM
        int fromIdx = upperSql.indexOf("FROM");
        String afterFrom = sql.substring(fromIdx + "FROM".length()).trim();
        String[] tokens = afterFrom.split("\s+");
        String tableName = tokens[0];

        Optional<SelectStatement.Condition> cond = Optional.empty();
        int whereIdx = upperSql.indexOf("WHERE");
        if (whereIdx >= 0) {
            String afterWhere = sql.substring(whereIdx + "WHERE".length()).trim();
            String[] condTokens = afterWhere.split("=", 2);
            String col = condTokens[0].trim();
            String val = condTokens[1].trim();
            // strip trailing semicolon if any
            if (val.endsWith(";")) val = val.substring(0, val.length() - 1).trim();
            // remove surrounding quotes
            if (val.startsWith("'") && val.endsWith("'")) {
                val = val.substring(1, val.length() - 1);
            }
            cond = Optional.of(new SelectStatement.Condition(col, val));
        }

        return new SelectStatement(tableName, cond);
    }
}
