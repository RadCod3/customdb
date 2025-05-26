package edu.mora.db.table;

import java.nio.ByteBuffer;

/**
 * A simple fixed‐length tuple: INT→4 bytes, STRING→length‐prefixed UTF‐8.
 */
public class Tuple {
    private final Schema schema;
    private final Object[] values;

    public Tuple(Schema schema, Object... vals) {
        if (vals.length != schema.numColumns())
            throw new IllegalArgumentException("Value count mismatch");
        this.schema = schema;
        this.values = vals;
    }

    public static Tuple deserialize(Schema schema, byte[] data) {
        ByteBuffer buf = ByteBuffer.wrap(data);
        Object[] vals = new Object[schema.numColumns()];
        for (int i = 0; i < schema.numColumns(); i++) {
            switch (schema.getColumnType(i)) {
                case INT:
                    vals[i] = buf.getInt();
                    break;
                case STRING:
                    int len = buf.getInt();
                    byte[] s = new byte[len];
                    buf.get(s);
                    vals[i] = new String(s);
                    break;
            }
        }
        return new Tuple(schema, vals);
    }

    public byte[] serialize() {
        // very naive: write each field in sequence
        ByteBuffer buf = ByteBuffer.allocate(4096);
        for (int i = 0; i < schema.numColumns(); i++) {
            switch (schema.getColumnType(i)) {
                case INT:
                    buf.putInt(Integer.parseInt(values[i].toString()));
                    break;
                case STRING:
                    byte[] s = ((String) values[i]).getBytes();
                    buf.putInt(s.length);
                    buf.put(s);
                    break;
            }
        }
        buf.flip();
        byte[] data = new byte[buf.limit()];
        buf.get(data);
        return data;
    }

    public Object getField(int i) {
        return values[i];
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        for (int i = 0; i < schema.numColumns(); i++) {
            if (i > 0) sb.append(", ");
            sb.append(values[i]);
        }
        sb.append(')');
        return sb.toString();
    }
}