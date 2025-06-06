package edu.mora.db.table;

import java.util.List;

public class Schema {
    private final List<String> columnNames;
    private final List<Type> columnTypes;

    public Schema(List<String> names, List<Type> types) {
        if (names.size() != types.size())
            throw new IllegalArgumentException("Names/types length mismatch");
        this.columnNames = names;
        this.columnTypes = types;
    }

    public int numColumns() {
        return columnNames.size();
    }

    public String getColumnName(int i) {
        return columnNames.get(i);
    }

    public Type getColumnType(int i) {
        return columnTypes.get(i);
    }

    public int getColumnIndex(String columnName) {
        int idx = columnNames.indexOf(columnName);
        if (idx < 0) {
            throw new IllegalArgumentException("Unknown column: " + columnName);
        }
        return idx;
    }

    public enum Type {INT, STRING}

    public List<String> getColumnNames() {
        return List.copyOf(columnNames);
    }
}